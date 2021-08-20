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

Why?
----

The existing `awscli <https://pypi.org/project/awscli/>`__ tool does not support autocompletion.
If you don't know the exact key, you need to look it up first, using an additional command::

    $ aws s3 ls s3://bucket/
    2018-07-12 20:22:15        575 key.yaml
    $ aws s3 cp s3://bucket/key.yaml -
    ...

If the key is long, you still need to type it all in::

    $ aws s3 ls s3://thesimpsons/apu
    2018-07-12 20:22:15     123456 apu_nahasapeemapetilon.png
    $ aws s3 cp s3://thesimpsons/apu_nahasapeemapetilon.png -
    ...

Another problem is dealing with non-standard endpoints, like localstack.
You need to specify the endpoint URL for each command, e.g.::

    $ aws --endpoint-url https://localhost:4566 s3 cp s3://local/hello.txt -
    hello world!

If you're lazy, and access S3 via the CLI often, then the above problems are a pain point.
`kot` solves them with autocompletion and an optional configuration file::

    $ kot s3://bucket/{tab}
    //key.yaml
    $ kot s3://thesimpsons/apu{tab}
    //apu_nahasapeemapetilon.png
    $ kot s3://local/hello{tab}
    //hello.txt
    {enter}
    hello world!

Installation
------------

To install the latest version from PyPI::

    pip install koshka

To get autocompletion to work under bash::

    pip install argcomplete
    eval "$(register-python-argcomplete kot)"

See `argcomplete documentation <https://pypi.org/project/argcomplete/>`__ for information about other platforms.

Configuration (optional)
------------------------

You may tell `kot` which AWS profile and/or endpoint URL to use for its requests via a config file.
Put the config file in `$HOME/kot.cfg`.
An example::

    [s3://mybucket]
    endpoint_url = http://localhost:4566

    [s3://myotherbucket]
    profile_name = myprofile

The section names are interpreted as regular expressions.
So, in the above example, `kot` will use `http://localhost:4566` as the endpoint URL for handle all requests starting with `s3://mybucket`.
Similarly, it will use the `myprofile` AWS profile to handle all requests starting with `s3://myotherbucket`.
