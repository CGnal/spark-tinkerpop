Examples
=======

Spark-Tinkerpop comes bundled with a few examples to illustrate some ways in which the core code can be used in a project. All examples follow the same pattern: they receive a configuration, load some data, generate a Graph, do something with it and finally save. The saved data is then reloaded and shown in the log via a custom `OutputStream` for comparison with the expected outcome.

Each example application is referred to by name, which is used at runtime to know which scala class to run. The currently supported applications are:

| name    | description                                                                                                                |
|---------|----------------------------------------------------------------------------------------------------------------------------|
|titan    | Loads Amazon product cross-sell data and stores and reloads data from a Titan database, which can be configured separately |
|graphson | Loads Amason product cross-sell data and stores and reloads data using Tinkerpop's GraphSON format                         |

The basic application takes some arguments:

| name      | short-name | description                                                          | type       | default |
|-----------|:----------:|----------------------------------------------------------------------|:----------:|:-------:|
|input      |     i      | input data file                                                      | string     |         |
|hadoop     |     h      | hadoop config directory                                              | string     |         |
|lib-dir    |     l      | extra library directory                                              | string     |         |
|tear-down  |     d      | tears down the graph on completion                                   | boolean    |  false  |
|threads    |     t      | sets the number of spark threads in local mode                       | int        |    2    |
|memory     |     m      | sets the amount of executor memory                                   | int + unit |    2g   |
|partitions |     p      | sets the default number of partitions in each RDD                    | int        |    2    |
|shuffle    |     x      | sets the shuffle-fraction value in spark                             | double     |   0.1   |
|storage    |     s      | sets the storage-fraction value in spark                             | double     |   0.4   |
|kerberos   |     k      | kerberos data for user and keytab of the form `user@keytab-location` | string     |         |

Note that when `kerberos` is undefined, security is assumed to be off.