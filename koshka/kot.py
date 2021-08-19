#!/usr/bin/env python
"""
Like GNU cat, but with autocompletion for S3.

To get autocompletion to work under bash:

    eval $(register-python-argcomplete kot

See <https://pypi.org/project/argcomplete/> for more details.
"""
import argparse
import io
import urllib.parse
import os
import sys

import argcomplete  # type: ignore
import boto3  # type: ignore

_DEBUG = os.environ.get('KOT_DEBUG')

#
# TODO:
#
# - [ ] Handle local paths
# - [ ] Handle multiple AWS profiles
# - [ ] More command-line options for compatibility with GNU cat
#


def completer(prefix, parsed_args, **kwargs):
    try:
        parsed_url = urllib.parse.urlparse(prefix)

        assert parsed_url.scheme == 's3'

        client = boto3.client('s3')

        bucket = parsed_url.netloc
        if not parsed_url.path:
            response = client.list_buckets()
            candidates = [
                f'{parsed_url.scheme}://{b["Name"]}'
                for b in response['Buckets'] if b['Name'].startswith(bucket)
            ]
            return candidates

        response = client.list_objects(
            Bucket=bucket,
            Prefix=parsed_url.path.lstrip('/'),
            Delimiter='/',
        )
        candidates = [
            f'{parsed_url.scheme}://{bucket}/{thing["Key"]}'
            for thing in response.get('Contents', [])
        ]
        candidates += [
            f'{parsed_url.scheme}://{bucket}/{thing["Prefix"]}'
            for thing in response.get('CommonPrefixes', [])
        ]
        return candidates
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
        epilog="To get autocompletion to work under bash: eval $(register-python-argcomplete kot)",
    )
    parser.add_argument('url').completer = completer  # type: ignore
    argcomplete.autocomplete(parser, validator=validator)
    args = parser.parse_args()

    parsed_url = urllib.parse.urlparse(args.url)
    assert parsed_url.scheme == 's3'

    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')

    client = boto3.client('s3')
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
