EsCmd
=====

EsCmd is a CLI tool and sdk to manage an Elasctic cluster.

It's written in python and uses a fork from the [official python SDK](https://github.com/fbacchella/elasticsearch-py).

More documentation about the sdk can be found at [the python sdk doc](https://elasticsearch-py.readthedocs.io/en/master/).

The fork was needed to add support for asyncio (using the Python 3.4 API) and improve the exception management.

Howto install in a virtualenv
-----------------------------

    VENV=...
    export PYCURL_SSL_LIBRARY=..
    virtualenv $VENV
    $VENV/bin/python setup.py install
    
On a RedHat familly distribution, the following packages are needed:

    yum install python-virtualenv gcc openssl-devel libcurl-devel

and `PYCURL_SSL_LIBRARY` must be set to `nss`. If missing, installation will not be able to detect the good ssl library used. 

For keytab support (see later), one should also run:

    $VENV/bin/pip install gssapi

Usage
=====

CLI
---
EsCmd can be used a CLI for Elastic. Each command does a single action.

The general command line is

    escmd [args] noun [args] verb [args]

For each noun, there is a set of verbs that can apply to it. Each args section
apply to the preceding term. So `escmd -c someting index` is different from `escmd index -c someting`.

To get a list of noun that can be used, try `escmd -h`. For a list ov verb that
can be used with an object, try `escmd <noun> -h`.

Config file
===========

EsCmd use a `ini` file to store settings, a example is given in `sample_config.ini`.

It the environnement variable `ESCONFIG` is given, it will be used to find the config file.


Generic options
===============

The generic options for all noun and verbs are

    -h, --help            show this help message and exit
    -c CONFIG_FILE, --config=CONFIG_FILE
                          an alternative config file
    -d, --debug           The debug level

Noun options
============

Usually a noun option take a filter option that can define on what object it applies.

    -h, --help            show this help message and exit
    -i ID, --id=ID        object ID
    -n NAME, --name=NAME  object tag 'Name'
    -s SEARCH, --search=SEARCH
                        Filter using a search expression

The option id and name obvioulsy return single object. But search can return many. Usually verb will then fail but some 
(like export or list) will operate on each of them.



Kerberos support
----------------

EsCmd add improved support of keytab. It's configured in [kerberos] section
in the ini file:

    [kerberos]
    ccache=
    keytab=
    principal=

It allows EsCmd to load a kerberos identity from a keytab, using a custom principal. The ccache define where tickets will
be stored and can use alternative credential cache, for more information see [MIT's ccache types](http://web.mit.edu/Kerberos/krb5-latest/doc/basic/ccache_def.html#ccache-types).

It uses [Python's GSSAPI](https://pypi.python.org/pypi/gssapi) but it's imported only if needed, so installation is not mandatory.


List of Nouns
=============


