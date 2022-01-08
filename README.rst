kot - GNU cat over the network with autocompletion
==================================================

.. image:: https://raw.githubusercontent.com/mpenkov/koshka/master/matroskin.jpeg
  :target: https://en.wikipedia.org/wiki/Uncle_Fedya,_His_Dog,_and_His_Cat#Matroskin_the_Cat

Usage
-----

Autocompleting S3 bucket names::

    $ kot s3://my{tab}
    //mybucket      //mybucket1     //mybucket2

Autocompleting S3 prefixes::

    $ kot s3://mybucket/myf{tab}
    //mybucket/myfile0.txt      //mybucket/myfile0.json

Autocompleting S3 versions (for more about versions, see below)::

    $ kot s3://myversionedbucket/key{tab}
    2021-03-23T03:46:03  2021-06-25T01:35:06

Autocompleting S3 output prefixes::

    $ kot README.rst -o //mybucket/myf{tab}
    //mybucket/myfile0.txt      //mybucket/myfile0.json

Autocompleting RESTful API endpoints that speak JSON::

    $ kot https://example.com/api
    {
        "customers": "http://example.com/api/customers",
        "locations": "http://example.com/api/locations",
        "products": "http://example.com/api/products"
    }
    $ kot https://example.com/api/{tab}
    //example.com/api/customers
    //example.com/api/locations
    //example.com/api/products

Editing a remote file transparently (again, with autocompletion)::

    $ kote //mybucket/myfile0.txt{enter}
    {$EDITOR opens a copy of the file locally}
    {Once $EDITOR exits, the local file overwrites the remote destination}

Aliasing::

    $ kot data/README.md{tab}
    https://mydataserver.developers.mycompany.com/README.md

Use this for long-ish URLs that you access frequently.
See the configuration section below for alias settings.

Why?
----

The project initially focused on S3, but then expanded to HTTP/S as well.

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
    eval "$(kot --register)"
    eval "$(kote --register)"

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

    [https://mydataserver.developers.mycompany.com]
    alias = data

The section names are interpreted as regular expressions.
So, in the above example, `kot` will use `http://localhost:4566` as the endpoint URL for handle all requests starting with `s3://mybucket`.
Similarly, it will use the `myprofile` AWS profile to handle all requests starting with `s3://myotherbucket`.

S3 Object Versions
------------------

If the bucket supports versioning, `kot` will list the datestamps of each version, as opposed to the arbitrary version IDs assigned by AWS.
For example, you may see URLs like:

    s3://bucket/key?LastModified=2021-03-23T03:46:03

Under the covers, `kot` will convert that datestamp to a version ID before attempting to access the content.
This is because `kot` is designed for consumption by human eyeballs, unlike `boto3` and friends.
By seeing the datestamp, the human user is in a better position to decide which version to access.
There is an edge-case where two versions are less than a second apart, meaning their timestamps will be identical, but this is rare, and `kot` does not attempt to handle it.

In order to represent the version as part of the URL in the command-line interface, `kot` uses the querystring part of the URL, as this is intuitive to human users, and I could not think of better alternatives.
Unfortunately, S3 URLs don't have querystring components, and can actually contain the querystring separator character (?).
Fortunately, this is a rare edge case, and `kot` does not attempt to handle it.
