#!/usr/bin/env python
"""
Like GNU cat, but with autocompletion for S3.

To get autocompletion to work under bash:

    eval "$(register-python-argcomplete kot)"

or the slightly more terse:

    eval "$(kot --register)"

See <https://pypi.org/project/argcomplete/> for more details.
"""
import argparse
import configparser
import io
import urllib.parse
import re
import os
import subprocess
import sys

import argcomplete  # type: ignore
import boto3  # type: ignore
import smart_open  # type: ignore

_DEBUG = os.environ.get('KOT_DEBUG')

#
# TODO:
#
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


def s3_matches(prefix):
    parsed_url = urllib.parse.urlparse(prefix)
    client = s3_client(prefix)

    bucket, path = parse_s3_url(prefix)
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


def local_matches(prefix):
    try:
        if os.path.exists(prefix):
            return [prefix]

        subdir, start = os.path.split(prefix)
        return [
            os.path.join(subdir, f)
            for f in os.listdir(subdir) if f.startswith(start)
        ]
    except OSError:
        return [os.listdir()]


def completer(prefix, parsed_args, **kwargs):
    try:
        #
        # TODO: handle non-S3 URLs here
        #
        parsed_url = urllib.parse.urlparse(prefix)
        if parsed_url.scheme == 's3':
            return s3_matches(prefix)
        else:
            return local_matches(prefix)
    except Exception as err:
        argcomplete.warn(f'uncaught exception err: {err}')
        return []


def debug():
    prefix = sys.argv[1]
    result = completer(prefix, None)
    print('\n'.join(result))


def parse_s3_url(url):
    parsed_url = urllib.parse.urlparse(url)
    assert parsed_url.scheme == 's3'

    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    return bucket, key


def main():
    def validator(current_input, keyword_to_check_against):
        return True

    parser = argparse.ArgumentParser(
        description="Like GNU cat, but with autocompletion for S3.",
        epilog="To get autocompletion to work under bash: eval $(kot --register)",
    )
    parser.add_argument('urls', nargs="*").completer = completer  # type: ignore
    parser.add_argument('--register', action='store_true', help='integrate with the current shell')

    #
    # Inspired by curl.  GNU cat does not use -o, so it's OK to use it here.
    #
    parser.add_argument('-o', '--output', help='write output here instead of stdout').completer = completer
    argcomplete.autocomplete(parser, validator=validator)
    args = parser.parse_args()

    if args.register:
        #
        # Assume we're working with bash.  For now, other shells can do it the
        # hard way, e.g. https://github.com/kislyuk/argcomplete#activating-global-completion
        # or make a PR ;)
        #
        bash_fu = subprocess.check_output(['register-python-argcomplete', 'kot'])
        sys.stdout.buffer.write(bash_fu)
        return

    if not args.output or args.output == '-':
        writer = sys.stdout.buffer
    else:
        parsed_url = urllib.parse.urlparse(args.output)
        tp = {}
        if parsed_url.scheme == 's3':
            client = s3_client(args.output)
            tp['client'] = client
        writer = smart_open.open(args.output, 'wb', compression='disable', transport_params=tp)

    for url in args.urls:
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme == 's3':
            bucket, key = parse_s3_url(url)
            client = s3_client(url)
            body = client.get_object(Bucket=bucket, Key=key)['Body']
        else:
            body = open(url, 'rb')

        while True:
            buf = body.read(io.DEFAULT_BUFFER_SIZE)
            if buf:
                try:
                    writer.write(buf)
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
