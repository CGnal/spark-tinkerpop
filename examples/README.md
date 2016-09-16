Examples
=======

Spark-Tinkerpop comes bundled with a few examples to illustrate some ways in which the core code can be used in a project. All examples follow the same pattern: they receive a configuration, load some data, generate a Graph, do something with it and finally save. The saved data is then reloaded and shown in the log via a custom `OutputStream` for comparison with the expected outcome.

Each example application is referred to by name, which is used at runtime to know which scala class to run. The currently supported applications are:

| name      | description                                                                                                                |
|-----------|----------------------------------------------------------------------------------------------------------------------------|
|`titan`    | Loads Amazon product cross-sell data and stores and reloads data from a Titan database, which can be configured separately |
|`graphson` | Loads Amason product cross-sell data and stores and reloads data using Tinkerpop's GraphSON format                         |

The basic application takes some arguments:

| name        | short-name   | description                                                          | type       | default   |
|-------------|:------------:|----------------------------------------------------------------------|:----------:|:---------:|
|`input`      |     `i`      | input data file                                                      | string     |           |
|`hadoop`     |     `h`      | hadoop config directory                                              | string     |           |
|`lib-dir`    |     `l`      | extra library directory                                              | string     |           |
|`tear-down`  |     `d`      | tears down the graph on completion                                   | boolean    |  `false`  |
|`threads`    |     `t`      | sets the number of spark threads in local mode                       | int        |    `2`    |
|`memory`     |     `m`      | sets the amount of executor memory                                   | int + unit |    `2g`   |
|`partitions` |     `p`      | sets the default number of partitions in each RDD                    | int        |    `2`    |
|`shuffle`    |     `x`      | sets the shuffle-fraction value in spark                             | double     |   `0.1`   |
|`storage`    |     `s`      | sets the storage-fraction value in spark                             | double     |   `0.4`   |
|`kerberos`   |     `k`      | kerberos data for user and keytab of the form `user@keytab-location` | string     |           |

Note that when `kerberos` is undefined, security is assumed to be off.

Running
#######

The [`bin/`](bin/) directory in the `examples` project lists some scripts that can be used to run the examples more easily with little configuration. 

The [`local.sh`](bin/local.sh) script is meant to run the examples on a local spark instance using `sbt-run`, therefore usually requiring no security configuration, minimal resources and possibly (but not necessarily) a local HBase instance for Titan when using the `hbase` storage backend. The `input`, `lib-dir` and application `name` settings can be passed as arguments to scripts with `--input`, `--name` and `--lib-dir` switches respectively, whereas the rest of the Spark configurations are set as constants within the script itself.

Unlike `local.sh`, the [`submit.sh`](bin/submit.sh) script will run the example applications using spark-submit to start the `SparkContext`, making it easier to run the examples on a Yarn cluster which may or may not be secured behind Kerberos. All the Spark driver and executor settings are set as constants within the script an can be modified as necessary, depending on the type and size of the input data. Besides the settings allowed by `local.sh`, `submit.sh` also allows setting of a principal and keytab file with the `--principal` and `--keytab` switches respectively which can permit access on a Kerberos-secured cluster.