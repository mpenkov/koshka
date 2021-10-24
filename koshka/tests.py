"""Integration and system tests.

Require network access.
"""
import contextlib
import os
import subprocess
import sys
import time

import koshka.kot
import koshka.httpls
import koshka.s3

KOT = koshka.kot.__file__


def test_kot_local():
    out = subprocess.check_output([sys.executable, KOT, KOT])
    with open(KOT, 'rb') as fin:
        assert out == fin.read()


def test_kot_https():
    out = subprocess.check_output([sys.executable, KOT, 'https://example.com'])
    assert b'This domain is for use in illustrative examples in documents.' in out


def test_kot_s3():
    out = subprocess.check_output([sys.executable, KOT, 's3://commoncrawl/robots.txt'])
    assert b'User-Agent: *\n' in out


def test_s3_complete():
    #
    # This is a public bucket, it should always be accessible.
    #
    contents = koshka.s3.complete('s3://commoncrawl')
    assert 's3://commoncrawl/robots.txt' in contents


@contextlib.contextmanager
def webserver(port=8080, cwd=os.path.dirname(__file__)):
    try:
        webserver_sub = subprocess.Popen(
            [sys.executable, '-m', 'http.server', '8080'],
            cwd=os.path.dirname(__file__),
        )
        time.sleep(1)
        yield
    finally:
        webserver_sub.terminate()


def test_https_complete():
    with webserver():
        contents = koshka.httpls.complete('http://localhost:8080')
        assert 'http://localhost:8080/tests.py' in contents
