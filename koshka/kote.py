#!/usr/bin/env python
"""Edit a remote file as if it was local.

Downloads the remote file to a temporary file, opens the editor, and then
uploads the edited copy.
"""
import argparse
import functools
import io
import os
import subprocess
import sys
import tempfile
import urllib.parse

import argcomplete  # type: ignore
import smart_open  # type: ignore

from . import kot


EDITOR = os.environ.get('EDITOR', 'vim')


def main():
    def validator(current_input, keyword_to_check_against):
        return True

    parser = argparse.ArgumentParser(
        description="kot editor: edit a remote file as if it was local",
        epilog="To get autocompletion to work under bash: eval $(kote --register)",
    )
    parser.add_argument('url', nargs='?').completer = kot.completer  # type: ignore
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

    if os.path.isfile(args.url):
        subprocess.run([EDITOR, args.url])
        return

    parsed_url = urllib.parse.urlparse(args.url)
    assert parsed_url.scheme == 's3'

    clever_open = functools.partial(
        smart_open.open,
        compression='disable',
        transport_params={'client': kot.s3_client(args.url)},
    )
    with tempfile.NamedTemporaryFile() as tmp:
        with clever_open(args.url, 'rb') as fin:
            _cat(fin, tmp)

        statinfo = os.stat(tmp.name)
        subprocess.check_call([EDITOR, tmp.name])

        #
        # Skip upload if the file has not changed
        #
        if os.stat(tmp.name).st_mtime > statinfo.st_mtime:
            breakpoint()
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
