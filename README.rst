kot - GNU cat with autocompletion for S3
========================================

Usage
-----

Autocompleting bucket names::

    $ kot s3://my{tab}
    //mybucket      //mybucket1     //mybucket2

Autocompleting prefixes::

    $ kot s3://mybucket/myf{tab}
    //mybucket/myfile0.txt      //mybucket/myfile0.json

Installation
------------

To install the latest version from PyPI::

    pip install koshka

To get autocompletion to work under bash::

    pip install argcomplete
    eval "$(register-python-argcomplete kot)"

See `argcomplete documentation <https://pypi.org/project/argcomplete/>`__ for information about other platforms.
