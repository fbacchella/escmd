EsCmd
=====

EsCmd is a CLI tool and sdk to manage an Elasctic cluster.

It's written in python and uses a fork from the [official python SDK](https://github.com/fbacchella/elasticsearch-py).

More documentation about the sdk can be found at [the python sdk doc](https://elasticsearch-py.readthedocs.io/en/master/).

The fork was needed to add support for asyncio (using the Python 3.4 API) and improve the exception management. The needed
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

### template

#### list
#### dump
#### put

    -f TEMPLATE_FILE_NAME, --template_file=TEMPLATE_FILE_NAME

### node

### task

#### list

It return tasks list, formatted as a tree.

The field for each line are :

    nodename/task_id task_name duration start_time
    
Example:

```
$ escmd task list 
node01/CKdecxJaQcGWwpDVfbL46g:1126800  internal:index/shard/recovery/file_chunk 0:00:00.019773 2018-01-31 11:28:22
node02/rKRb9JDxQFepOlTa2T1qQg:691129   internal:index/shard/recovery/start_recovery 0:00:12.087730 2018-01-31 11:28:09
node01/CKdecxJaQcGWwpDVfbL46g:1067982  internal:index/shard/recovery/start_recovery 0:21:22.599715 2018-01-31 11:06:59
node01/CKdecxJaQcGWwpDVfbL46g:1126760  indices:monitor/stats          0:00:00.651505 2018-01-31 11:28:21
    node04/kvAXQzO2TeqJdX2Z64rCrw:610649   indices:monitor/stats[n]       0:00:00.631077 2018-01-31 11:28:21
    node05/n8lxNrV7T6OHjKqgUjzv_A:90165    indices:monitor/stats[n]       0:00:00.633928 2018-01-31 11:28:21
    node01/CKdecxJaQcGWwpDVfbL46g:1126762  indices:monitor/stats[n]       0:00:00.634895 2018-01-31 11:28:21
    node02/rKRb9JDxQFepOlTa2T1qQg:691579   indices:monitor/stats[n]       0:00:00.635082 2018-01-31 11:28:21
    node03/LBbza-nGQ2Cy_P1d_AOZog:716222   indices:monitor/stats[n]       0:00:00.632749 2018-01-31 11:28:21
    node06/FY2E8_uFT9uO-McpaA9oEg:378173   indices:monitor/stats[n]       0:00:00.630454 2018-01-31 11:28:21
    node07/gl3avq6hSKuxGEEQvQBPwQ:638464   indices:monitor/stats[n]       0:00:00.633831 2018-01-31 11:28:21
    node09/JVfMfP2aQx21FQonRkSO0w:609372   indices:monitor/stats[n]       0:00:00.633548 2018-01-31 11:28:21
node01/CKdecxJaQcGWwpDVfbL46g:1088546  internal:index/shard/recovery/start_recovery 0:13:49.510606 2018-01-31 11:14:32
node03/LBbza-nGQ2Cy_P1d_AOZog:716139   internal:index/shard/recovery/start_recovery 0:00:27.250931 2018-01-31 11:27:54
node03/LBbza-nGQ2Cy_P1d_AOZog:714502   internal:index/shard/recovery/start_recovery 0:06:46.695529 2018-01-31 11:21:35
node01/CKdecxJaQcGWwpDVfbL46g:1093511  internal:index/shard/recovery/start_recovery 0:11:31.164384 2018-01-31 11:16:50
node01/CKdecxJaQcGWwpDVfbL46g:1111813  internal:index/shard/recovery/start_recovery 0:06:36.767685 2018-01-31 11:21:45
node03/LBbza-nGQ2Cy_P1d_AOZog:716226   indices:data/write/bulk[s]     0:00:00.201633 2018-01-31 11:28:21
    node07/gl3avq6hSKuxGEEQvQBPwQ:638468   indices:data/write/bulk[s]     0:00:00.194595 2018-01-31 11:28:21
        node09/JVfMfP2aQx21FQonRkSO0w:609380   indices:data/write/bulk[s][r]  0:00:00.065229 2018-01-31 11:28:22
        node07/gl3avq6hSKuxGEEQvQBPwQ:638469   indices:data/write/bulk[s][p]  0:00:00.194504 2018-01-31 11:28:21
node01/CKdecxJaQcGWwpDVfbL46g:1088201  internal:index/shard/recovery/start_recovery 0:13:58.161292 2018-01-31 11:14:23
node03/LBbza-nGQ2Cy_P1d_AOZog:715299   internal:index/shard/recovery/start_recovery 0:04:22.566121 2018-01-31 11:23:59
node05/n8lxNrV7T6OHjKqgUjzv_A:90175    internal:index/shard/recovery/prepare_translog 0:00:00.017585 2018-01-31 11:28:22
node03/LBbza-nGQ2Cy_P1d_AOZog:716224   indices:data/write/bulk        0:00:00.209870 2018-01-31 11:28:21
node01/CKdecxJaQcGWwpDVfbL46g:1120268  internal:index/shard/recovery/start_recovery 0:02:21.984695 2018-01-31 11:26:00
node03/LBbza-nGQ2Cy_P1d_AOZog:715964   internal:index/shard/recovery/start_recovery 0:01:19.430565 2018-01-31 11:27:02
node01/CKdecxJaQcGWwpDVfbL46g:1126801  internal:index/shard/recovery/file_chunk 0:00:00.011332 2018-01-31 11:28:22
node01/CKdecxJaQcGWwpDVfbL46g:1060600  internal:index/shard/recovery/start_recovery 0:23:46.772189 2018-01-31 11:04:35
node06/FY2E8_uFT9uO-McpaA9oEg:378178   cluster:monitor/tasks/lists    0:00:00.000639 2018-01-31 11:28:22
    node06/FY2E8_uFT9uO-McpaA9oEg:378179   cluster:monitor/tasks/lists[n] 0:00:00.000197 2018-01-31 11:28:22
    node03/LBbza-nGQ2Cy_P1d_AOZog:716232   cluster:monitor/tasks/lists[n] 0:00:00.000270 2018-01-31 11:28:22
    node05/n8lxNrV7T6OHjKqgUjzv_A:90177    cluster:monitor/tasks/lists[n] 0:00:00.000286 2018-01-31 11:28:22
    node04/kvAXQzO2TeqJdX2Z64rCrw:610654   cluster:monitor/tasks/lists[n] 0:00:00.000297 2018-01-31 11:28:22
    node08/C2xRwnkeSAmRQql7ecZwqA:6559     cluster:monitor/tasks/lists[n] 0:00:00.000212 2018-01-31 11:28:22
    node01/CKdecxJaQcGWwpDVfbL46g:1126803  cluster:monitor/tasks/lists[n] 0:00:00.000810 2018-01-31 11:28:22
    node07/gl3avq6hSKuxGEEQvQBPwQ:638474   cluster:monitor/tasks/lists[n] 0:00:00.000236 2018-01-31 11:28:22
    node02/rKRb9JDxQFepOlTa2T1qQg:691612   cluster:monitor/tasks/lists[n] 0:00:00.000302 2018-01-31 11:28:22
    node09/JVfMfP2aQx21FQonRkSO0w:609382   cluster:monitor/tasks/lists[n] 0:00:00.000285 2018-01-31 11:28:22
node03/LBbza-nGQ2Cy_P1d_AOZog:715234   internal:index/shard/recovery/start_recovery 0:04:37.901974 2018-01-31 11:23:44
node03/LBbza-nGQ2Cy_P1d_AOZog:716229   indices:data/write/bulk[s]     0:00:00.191351 2018-01-31 11:28:21
    node02/rKRb9JDxQFepOlTa2T1qQg:691603   indices:data/write/bulk[s]     0:00:00.184870 2018-01-31 11:28:21
        node02/rKRb9JDxQFepOlTa2T1qQg:691604   indices:data/write/bulk[s][p]  0:00:00.184761 2018-01-31 11:28:21
        node09/JVfMfP2aQx21FQonRkSO0w:609379   indices:data/write/bulk[s][r]  0:00:00.074169 2018-01-31 11:28:22
node01/CKdecxJaQcGWwpDVfbL46g:1094866  internal:index/shard/recovery/start_recovery 0:11:12.373662 2018-01-31 11:17:09
node01/CKdecxJaQcGWwpDVfbL46g:1067285  internal:index/shard/recovery/start_recovery 0:21:50.082148 2018-01-31 11:06:32
node01/CKdecxJaQcGWwpDVfbL46g:1079577  internal:index/shard/recovery/start_recovery 0:18:07.769536 2018-01-31 11:10:14
node03/LBbza-nGQ2Cy_P1d_AOZog:716228   indices:data/write/bulk[s]     0:00:00.194622 2018-01-31 11:28:21
    node09/JVfMfP2aQx21FQonRkSO0w:609377   indices:data/write/bulk[s]     0:00:00.188370 2018-01-31 11:28:21
        node07/gl3avq6hSKuxGEEQvQBPwQ:638472   indices:data/write/bulk[s][r]  0:00:00.065661 2018-01-31 11:28:22
        node09/JVfMfP2aQx21FQonRkSO0w:609378   indices:data/write/bulk[s][p]  0:00:00.188184 2018-01-31 11:28:21
node04/kvAXQzO2TeqJdX2Z64rCrw:610241   internal:index/shard/recovery/start_recovery 0:00:43.207200 2018-01-31 11:27:38
node03/LBbza-nGQ2Cy_P1d_AOZog:716227   indices:data/write/bulk[s]     0:00:00.197915 2018-01-31 11:28:21
    node02/rKRb9JDxQFepOlTa2T1qQg:691600   indices:data/write/bulk[s]     0:00:00.192430 2018-01-31 11:28:21
        node02/rKRb9JDxQFepOlTa2T1qQg:691601   indices:data/write/bulk[s][p]  0:00:00.192326 2018-01-31 11:28:21
        node07/gl3avq6hSKuxGEEQvQBPwQ:638471   indices:data/write/bulk[s][r]  0:00:00.070751 2018-01-31 11:28:22
node03/LBbza-nGQ2Cy_P1d_AOZog:715941   internal:index/shard/recovery/start_recovery 0:01:26.524735 2018-01-31 11:26:55
```

#### dump

Dump a task details

```
escmd task -i kvAXQzO2TeqJdX2Z64rCrw:644452 dump -p
{
  "kvAXQzO2TeqJdX2Z64rCrw:644452": {
    "action": "internal:index/shard/recovery/start_recovery",
    "id": 644452,
    "node": "kvAXQzO2TeqJdX2Z64rCrw",
    "running_time_in_nanos": 5401906513,
    "start_time_in_millis": 1517396689660,
    "type": "netty"
  }
}
```

Or:
```
escmd task dump -p type
{
  "gl3avq6hSKuxGEEQvQBPwQ:689282": "netty"
}
{
  "kvAXQzO2TeqJdX2Z64rCrw:644717": "direct"
}
{
  "LBbza-nGQ2Cy_P1d_AOZog:751153": "netty"
}
...
```

### index

#### list
#### reindex

    -t TEMPLATE_NAME, --use_template=TEMPLATE_NAME
                        Template to use for reindexing
    -p PREFIX, --prefix=PREFIX
    -s SUFFIX, --suffix=SUFFIX
    -i INFIX_REGEX, --infix_regex=INFIX_REGEX

Force the reindex of a index. If `--use_template`, the given template will be used for mapping, otherwise, it will use the
same than the old index. Aliases as kept from the old index.

The name of the new index will be `prefix` + `infix` + `suffix`. The infix is build using a regex that will extract
if from the reindexed index, the first group of the regex will be used. For example, for the index named
`prefix.monitoring-es-6-2018.01.29_1` and the regex `(?:.*)(.monitoring-es-6-..........).*`, the infix will be 
`monitoring-es-6-2018.01.29`. Prefix and suffix are taken directly from the arguments.

#### readsettings

    -k, --only_keys  
    -p, --pretty     
    -f, --flat       

If `--flat` is given results are printed as a list of `indexname/setting=value`. The default format is a sequence of
json object, one for each index.

#### writesettings

    -f SETTINGS_FILE_NAME, --settings_file=SETTINGS_FILE_NAME

It take a sequence of `setting=value` that will be applied to each given index. If `--settings_file` is given, the
settings applied will be taken from this json file.

#### dump
#### delete
#### forcemerge

    -m MAX_NUM_SEGMENTS, --max_num_segments=MAX_NUM_SEGMENTS
                          Max num segments

### cluster

#### readsettings

Used to display settings.

    Options:
      -h, --help       show this help message and exit
      -k, --only_keys  
      -p, --pretty     
      -f, --flat       

It can take a path to sub-settings, for easier search. Without `--flat`, results are returned as a json object. With it,
each value is returned flattened from start

For examples:

    $ escmd cluster readsettings -f | sort -V
    action.auto_create_index: true
    action.destructive_requires_name: false
    action.master.force_local: false
    ...
    xpack.watcher.transform.search.default_timeout: 
    xpack.watcher.trigger.schedule.ticker.tick_interval: 500ms
    xpack.watcher.watch.scroll.size: 0

    $ escmd cluster readsettings -p
    persistent: {}
    defaults: {
      "action": {
        "auto_create_index": "true",
    ...
          "watch": {
            "scroll": {
              "size": "0"
            }
          }
        }
      }
    }
    transient: {}
