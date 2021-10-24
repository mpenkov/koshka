#!/usr/bin/env python
"""Edit a remote file as if it was local.

Downloads the remote file to a temporary file, opens the editor, and then
uploads the edited copy.
"""
import argparse
import contextlib
import io
import os
import subprocess
import sys
import tempfile
import urllib.parse

import argcomplete  # type: ignore

import koshka.kot
import koshka.s3


EDITOR = os.environ.get('EDITOR', 'vim')


@contextlib.contextmanager
def clever_open(url, mode):
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme == 's3':
        body = koshka.s3.open(url, mode)
    elif parsed_url.scheme in ('http', 'https'):
        body = koshka.httpls.open(url)
    else:
        body = open(url, mode)
    yield body


def main():
    def validator(current_input, keyword_to_check_against):
        return True

    parser = argparse.ArgumentParser(
        description="kot editor: edit a remote file as if it was local",
        epilog="To get autocompletion to work under bash: eval $(kote --register)",
    )
    parser.add_argument('url', nargs='?').completer = koshka.kot.completer  # type: ignore
    parser.add_argument('-R', '--readonly', action='store_true')
    parser.add_argument('--register', action='store_true', help='integrate with the current shell')

    argcomplete.autocomplete(parser, validator=validator)
    args = parser.parse_args()

    if args.register:
        #
        # Assume we're working with bash.  For now, other shells can do it the
        # hard way, e.g. https://github.com/kislyuk/argcomplete#activating-global-completion
        # or make a PR ;)
        #
        bash_fu = subprocess.check_output(['register-python-argcomplete', 'kote'])
        sys.stdout.buffer.write(bash_fu)
        return

    if not args.url:
        parser.error('I need a URL to edit')

    parsed_url = urllib.parse.urlparse(args.url)
    if parsed_url.scheme in ('http', 'https'):
        args.readonly = True

    _, filename = os.path.split(parsed_url.path)
    prefix, suffix = os.path.splitext(filename)
    with tempfile.NamedTemporaryFile(prefix=prefix + '-', suffix=suffix) as tmp:
        with clever_open(args.url, 'rb') as fin:
            _cat(fin, tmp)
        tmp.flush()

        statinfo = os.stat(tmp.name)

        if args.readonly:
            os.chmod(tmp.name, 0o400)

        subprocess.check_call([EDITOR, tmp.name])

        #
        # Skip upload if the file has not changed
        #
        if not args.readonly and os.stat(tmp.name).st_mtime > statinfo.st_mtime:
            with open(tmp.name, 'rb') as fin:
                with clever_open(args.url, 'wb') as fout:
                    _cat(fin, fout)


def _cat(fin, fout):
    while True:
        buf = fin.read(io.DEFAULT_BUFFER_SIZE)
        if not buf:
            break
        fout.write(buf)
    fout.flush()


if __name__ == '__main__':
    main()
