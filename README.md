EsCmd
=====

EsCmd is a CLI tool and sdk to manage an Elasctic cluster.

It's written in python and uses a fork from the [official python SDK](https://github.com/fbacchella/elasticsearch-py).

More documentation about the sdk can be found at [the python sdk doc](https://elasticsearch-py.readthedocs.io/en/master/).

The fork was needed to add support for [asyncio](https://docs.python.org/3/library/asyncio.html) (using the Python 3.4 API) and improve the exception management. The needed
branch is on https://github.com/fbacchella/elasticsearch-py/tree/Async. It can be installed with:

    pip install git+https://github.com/fbacchella/elasticsearch-py.git@Async

Howto install in a virtualenv
-----------------------------

    VENV=...
    export PYCURL_SSL_LIBRARY=..
    virtualenv-3 $VENV
    $VENV/bin/pip install git+https://github.com/fbacchella/elasticsearch-py.git@Async
    git clone https://github.com/fbacchella/escmd.git
    cd escmd
    $VENV/bin/python setup.py install
    
On a RedHat familly distribution, the following packages are needed:

    yum install python34-virtualenv gcc openssl-devel libcurl-devel

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
can be used with an noun, try `escmd <noun> -h`.

Config file
===========

EsCmd use a `ini` file to store settings. The most basic ini will will be:

```
[api]
url=localhost:9200
sniff=False
```

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

Kerberos support
----------------

EsCmd can use Kerberos and keytab for SSO authentication. It's actived by adding `kerberos=True` in the  `[api]` section .

The keytab settings is configured in an optionnal `[kerberos]` section
in the ini file:

    [kerberos]
    ccache=
    keytab=
    principal=

It allows EsCmd to load a kerberos identity from a keytab, using a custom principal. The ccache define where tickets will
be stored and can use alternative credential cache, for more information see [MIT's ccache types](http://web.mit.edu/Kerberos/krb5-latest/doc/basic/ccache_def.html#ccache-types).

It uses [Python's GSSAPI](https://pypi.python.org/pypi/gssapi) but it's imported only if needed, so installation is not mandatory.


Noun and verbs
--------------

Documentation about noun and verbs is to be found in the [wiki](https://github.com/fbacchella/escmd/wiki/List-of-Nouns)

Cat noun
--------

Some noun got a `cat` verb, that maps to the cat REST api. They all share a same set of options:
```
  -H HEADERS, --headers=HEADERS, default to '*'
  -f FORMAT, --format=FORMAT, can be 'text' or 'json', the elasticsearch-py don't hanlde yaml output
  -p, --pretty          
  -l, --local           
```

Build status
------------

[![Build Status](https://api.travis-ci.org/fbacchella/escmd.png)](https://travis-ci.org/fbacchella/escmd)


License
-------

Copyright 2018 Fabrice Bacchella

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

