import configparser
import html.parser
import json
import os
import re

import requests
import smart_open  # type: ignore

from typing import (
    List,
    Optional,
    Tuple,
)


class _HtmlParser(html.parser.HTMLParser):
    def __init__(self):
        super(_HtmlParser, self).__init__()
        self.hyperlinks = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for (name, value) in attrs:
                if name == 'href' and not value.startswith('?'):
                    self.hyperlinks.append(value)
                    break


def _auth(prefix: str) -> Optional[Tuple[str, str]]:
    """Load HTTP authentication from the global config for the specified prefix."""
    try:
        parser = configparser.ConfigParser()
        parser.read(os.path.expanduser('~/kot.cfg'))
        for section in parser.sections():
            if re.match(section, prefix):
                try:
                    return parser[section]['username'], parser[section]['password']
                except KeyError:
                    pass
    except IOError:
        pass

    return None


def _traverse_json(obj, prefix, urls):
    """Cherry-pick any URLs beginning with the prefix."""
    if isinstance(obj, str) and obj.startswith(prefix):
        urls.append(obj)
    elif isinstance(obj, list):
        for elem in obj:
            _traverse_json(elem, prefix, urls)
    elif isinstance(obj, dict):
        for elem in obj.values():
            _traverse_json(elem, prefix, urls)


def _complete(prefix):
    if not prefix.endswith('/'):
        prefix += '/'

    get = requests.get(prefix, auth=_auth(prefix))
    get.raise_for_status()

    if 'json' in get.headers['Content-Type']:
        document = json.loads(get.text)
        urls = list()
        _traverse_json(document, prefix, urls)
        return sorted(set(urls))

    parser = _HtmlParser()
    parser.feed(get.text)
    hyperlinks = sorted(set(parser.hyperlinks))
    #
    # FIXME: check if h is relative or absolute
    #
    return [prefix + h for h in hyperlinks]


def complete(prefix: str) -> List[str]:
    #
    # Try the prefix as-is first, because it may actually be a
    # complete URL.  Otherwise, if it's incomplete, try the directory
    # listing of the parent.
    #
    try:
        links = _complete(prefix)
        if links:
            return links
        else:
            return [prefix]
    except Exception:
        pass

    parent_prefix = prefix[:prefix.rindex('/') + 1]
    return [
        url
        for url in _complete(parent_prefix)
        if url.startswith(prefix)
    ]


def open(url):
    tp = {}
    try:
        username, password = _auth(url)
    except Exception:
        pass
    else:
        tp['user'] = username
        tp['password'] = password
    return smart_open.open(url, mode='rb', transport_params=tp, compression='disable')


if __name__ == '__main__':
    import sys
    print(complete(sys.argv[1]))
