import configparser
import os
import re
import urllib


import boto3  # type: ignore
import smart_open


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


def _list_bucket(client, scheme, bucket, prefix, delimiter='/'):
    response = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    candidates = [
        f'{scheme}://{bucket}/{thing["Key"]}'
        for thing in response.get('Contents', [])
    ]
    candidates += [
        f'{scheme}://{bucket}/{thing["Prefix"]}'
        for thing in response.get('CommonPrefixes', [])
    ]
    return candidates


def _parse(url):
    parsed_url = urllib.parse.urlparse(url)
    assert parsed_url.scheme == 's3'

    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    return bucket, key


def _matches(prefix):
    parsed_url = urllib.parse.urlparse(prefix)
    client = _client(prefix)

    bucket, path = _parse(prefix)
    if not path:
        response = client.list_buckets()
        buckets = [
            b['Name']
            for b in response['Buckets'] if b['Name'].startswith(bucket)
        ]
        if len(buckets) == 0:
            return []
        elif len(buckets) > 1:
            urls = [f'{parsed_url.scheme}://{bucket}' for bucket in buckets]
            return urls
        else:
            bucket = buckets[0]
            path = ''

    return _list_bucket(client, parsed_url.scheme, bucket, path)


def complete(prefix):
    parsed_url = urllib.parse.urlparse(prefix)
    client = _client(prefix)
    bucket = parsed_url.netloc
    path = parsed_url.path.lstrip('/')
    if not path:
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

    return _list_bucket(client, parsed_url.scheme, bucket, path)


def open(url, mode):
    transport_params = {'client': _client(url)}
    return smart_open.open(url, mode, transport_params=transport_params)
