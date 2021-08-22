#!/usr/bin/env python
"""
Like GNU cat, but with autocompletion for S3.

To get autocompletion to work under bash:

    eval "$(register-python-argcomplete kot)"

See <https://pypi.org/project/argcomplete/> for more details.
"""
import argparse
import configparser
import io
import urllib.parse
import re
import os
import sys

import argcomplete  # type: ignore
import boto3  # type: ignore

_DEBUG = os.environ.get('KOT_DEBUG')

#
# TODO:
#
# - [ ] Handle local paths
# - [ ] More command-line options for compatibility with GNU cat
#


def s3_client(prefix):
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


def list_bucket(client, scheme, bucket, prefix, delimiter='/'):
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


def completer(prefix, parsed_args, **kwargs):
    try:
        parsed_url = urllib.parse.urlparse(prefix)

        assert parsed_url.scheme == 's3'

        client = s3_client(prefix)

        bucket = parsed_url.netloc
        path = parsed_url.path.lstrip('/')
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

        return list_bucket(client, parsed_url.scheme, bucket, path)
    except Exception as err:
        argcomplete.warn(f'uncaught exception err: {err}')
        return []


def debug():
    prefix = sys.argv[1]
    result = completer(prefix, None)
    print('\n'.join(result))


def main():
    def validator(current_input, keyword_to_check_against):
        return True

    parser = argparse.ArgumentParser(
        description="Like GNU cat, but with autocompletion for S3.",
        epilog="To get autocompletion to work under bash: eval \"$(register-python-argcomplete kot)\"",
    )
    parser.add_argument('url').completer = completer  # type: ignore
    argcomplete.autocomplete(parser, validator=validator)
    args = parser.parse_args()

    parsed_url = urllib.parse.urlparse(args.url)
    assert parsed_url.scheme == 's3'

    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')

    client = s3_client(args.url)
    body = client.get_object(Bucket=bucket, Key=key)['Body']

    while True:
        buf = body.read(io.DEFAULT_BUFFER_SIZE)
        if buf:
            try:
                sys.stdout.buffer.write(buf)
            except BrokenPipeError:
                #
                # https://stackoverflow.com/questions/26692284/how-to-prevent-brokenpipeerror-when-doing-a-flush-in-python
                #
                sys.stderr.close()
                sys.exit(0)
        else:
            break


if __name__ == '__main__' and _DEBUG:
    #
    # For debugging the completer.
    #
    debug()
elif __name__ == '__main__':
    main()
