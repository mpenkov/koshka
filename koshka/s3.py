import configparser
import os
import re
import urllib


import boto3  # type: ignore
import smart_open  # type: ignore

_FMT = '%Y-%m-%dT%H:%M:%S'


def _client(prefix):
    endpoint_url = profile_name = None
    try:
        parser = configparser.ConfigParser()
        parser.read(os.path.expanduser('~/kot.cfg'))
        for section in parser.sections():
            if re.match(section, prefix):
                endpoint_url = parser[section].get('endpoint_url') or None
                profile_name = parser[section].get('profile_name') or None
    except IOError:
        pass

    session = boto3.Session(profile_name=profile_name)
    return session.client('s3', endpoint_url=endpoint_url)


def _list_bucket(client, scheme, bucket, prefix, querystr='', delimiter='/'):
    response = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    keys = [
        f'{scheme}://{bucket}/{thing["Key"]}'
        for thing in response.get('Contents', [])
    ]
    prefixes = [
        f'{scheme}://{bucket}/{thing["Prefix"]}'
        for thing in response.get('CommonPrefixes', [])
    ]
    candidates = keys + prefixes

    def versioning_enabled():
        try:
            return client.get_bucket_versioning(Bucket=bucket)['Status'] == 'Enabled'
        except KeyError:
            return False

    if len(candidates) == len(keys) == 1 and versioning_enabled():
        #
        # If this bucket is versioned, look for multiple versions of this file.
        #
        response = client.list_object_versions(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
        )
        if response['Versions']:
            datestamps = [
                v['LastModified'].strftime(_FMT)
                for v in response['Versions']
            ]
            querystrings = [f'LastModified={dt}' for dt in datestamps]
            querystrings = [q for q in querystrings if q.startswith(querystr)]
            return [f'{candidates[0]}?{q}' for q in querystrings]
    return candidates


def complete(prefix):
    parsed_url = urllib.parse.urlparse(prefix)
    client = _client(prefix)
    bucket = parsed_url.netloc
    path = parsed_url.path.lstrip('/')
    if not path:
        #
        # Perform bucket name completion.
        #
        response = client.list_buckets()
        buckets = [
            b['Name']
            for b in response['Buckets'] if b['Name'].startswith(bucket)
        ]
        #
        # Publicly visible buckets won't show up in list_buckets, so we should
        # try accessing it explicitly below.
        #
        if len(buckets) == 0:
            pass
        elif len(buckets) > 1:
            urls = [f'{parsed_url.scheme}://{bucket}' for bucket in buckets]
            return urls
        else:
            bucket = buckets[0]
            path = ''

    return _list_bucket(client, parsed_url.scheme, bucket, path, parsed_url.query)


def _to_version_id(client, bucket: str, prefix: str, timestamp: str, delimiter='/') -> str:
    response = client.list_object_versions(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter=delimiter,
    )
    for v in response['Versions']:
        if v['LastModified'].strftime(_FMT) == timestamp:
            return v['VersionId']
    assert False


def open(url, mode):
    client = _client(url)
    transport_params = {'client': client}
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.query and parsed_url.query.startswith('LastModified='):
        timestamp = parsed_url.query.replace('LastModified=', '')
        bucket = parsed_url.hostname
        prefix = parsed_url.path.lstrip('/')
        transport_params['version_id'] = _to_version_id(
            client,
            bucket,
            prefix,
            timestamp,
        )
        url = urllib.parse.urlunparse(parsed_url._replace(query=''))
    return smart_open.open(
        url,
        mode,
        transport_params=transport_params,
        compression='disable',
    )


def main():
    import sys
    print(complete(sys.argv[1]))


if __name__ == '__main__':
    main()
