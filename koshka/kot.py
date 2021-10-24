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
import os
import subprocess
import sys

import argcomplete  # type: ignore
import smart_open  # type: ignore

import koshka.httpls
import koshka.s3

_DEBUG = os.environ.get('KOT_DEBUG')


#
# TODO:
#
# - [ ] More command-line options for compatibility with GNU cat
#
def _dealias(prefix: str) -> str:
    try:
        parser = configparser.ConfigParser()
        parser.read(os.path.expanduser('~/kot.cfg'))
        for section in parser.sections():
            try:
                alias = parser[section]['alias']
            except KeyError:
                continue

            if prefix.startswith(alias):
                return prefix.replace(alias, section)
    except IOError:
        pass

    return prefix


def completer(prefix, parsed_args, **kwargs):
    prefix = _dealias(prefix)
    try:
        parsed_url = urllib.parse.urlparse(prefix)

        if parsed_url.scheme == 's3':
            return koshka.s3.complete(prefix)

        if parsed_url.scheme in ('http', 'https'):
            return koshka.httpls.complete(prefix)

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
        epilog="To get autocompletion to work under bash: eval $(kot --register)",
    )
    parser.add_argument('urls', nargs="*").completer = completer  # type: ignore
    parser.add_argument('--register', action='store_true', help='integrate with the current shell')

    #
    # Inspired by curl.  GNU cat does not use -o, so it's OK to use it here.
    #
    parser.add_argument(
        '-o',
        '--output',
        help='write output here instead of stdout',
    ).completer = completer
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
            client = koshka.s3._client(args.output)
            tp['client'] = client
        writer = smart_open.open(args.output, 'wb', compression='disable', transport_params=tp)

    for url in args.urls:
        url = _dealias(url)
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme == 's3':
            body = koshka.s3.open(url, 'rb')
        elif parsed_url.scheme in ('http', 'https'):
            body = koshka.httpls.open(url)
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
