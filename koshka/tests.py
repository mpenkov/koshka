"""Integration and system tests.

Require network access.
"""
import contextlib
import os
import subprocess
import sys
import time

import pytest

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
def webserver(port=8888, cwd=os.path.dirname(__file__)):
    try:
        webserver_sub = subprocess.Popen(
            [sys.executable, '-m', 'http.server', str(port)],
            cwd=os.path.dirname(__file__),
        )
        time.sleep(1)
        yield
    finally:
        webserver_sub.terminate()


def test_https_complete():
    with webserver():
        contents = koshka.httpls.complete('http://localhost:8888')
        assert 'http://localhost:8888/tests.py' in contents


def test_traverse_json():
    prefix = "https://example.com/api"
    obj = {
        "customers": f"{prefix}/customers",
        "locations": f"{prefix}/locations",
        "products": f"{prefix}/products",
        "continents": [
            f"{prefix}/continents/africa",
            f"{prefix}/continents/asia",
            f"{prefix}/continents/etc",
        ],
        "currencies": [
            {"name": "dollar", "url": f'{prefix}/currencies/dollar'},
            {"name": "rouble", "url": f'{prefix}/currencies/rouble'},
            {"name": "yen", "url": f'{prefix}/currencies/yen'},
        ]
    }
    urls = []

    koshka.httpls._traverse_json(obj, prefix, urls)

    assert f'{prefix}/customers' in urls
    assert f'{prefix}/continents/africa' in urls
    assert f'{prefix}/currencies/rouble' in urls


@pytest.mark.skip('dummy web API not working yet')
def test_https_complete_rest():
    with webserver():
        contents = koshka.httpls.complete('http://localhost:8888/sampleapi.json')
        assert 'http://localhost:8888/sampleapi/customers' in contents
        assert 'http://localhost:8888/sampleapi/locations' in contents
        assert 'http://localhost:8888/sampleapi/products' in contents
