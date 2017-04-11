Welcome to Livy
===============

.. image:: https://travis-ci.org/cloudera/livy.svg?branch=master
    :target: https://travis-ci.org/cloudera/livy

Livy is an open source REST interface for interacting with `Apache Spark`_ from anywhere.
It supports executing snippets of code or programs in a Spark context that runs locally or in `Apache Hadoop YARN`_.

* Interactive Scala, Python and R shells
* Batch submissions in Scala, Java, Python
* Multiple users can share the same server (impersonation support)
* Can be used for submitting jobs from anywhere with REST
* Does not require any code change to your programs

`Pull requests`_ are welcomed! But before you begin, please check out the `Wiki`_.

.. _Apache Spark: http://spark.apache.org
.. _Apache Hadoop YARN: http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _Pull requests: https://github.com/cloudera/livy/pulls
.. _Wiki: https://github.com/cloudera/livy/wiki/Contributing-to-Livy


Prerequisites
=============

To build Livy, you will need:

Debian/Ubuntu:
  * mvn (from ``maven`` package or maven3 tarball)
  * openjdk-7-jdk (or Oracle Java7 jdk)
  * Python 2.6+
  * R 3.x

Redhat/CentOS:
  * mvn (from ``maven`` package or maven3 tarball)
  * java-1.7.0-openjdk (or Oracle Java7 jdk)
  * Python 2.6+
  * R 3.x

MacOS:
  * Xcode command line tools
  * Oracle's JDK 1.7+
  * Maven (Homebrew)
  * Python 2.6+
  * R 3.x

Required python packages for building Livy:
  * cloudpickle
  * requests
  * requests-kerberos
  * flake8
  * flaky
  * pytest


To run Livy, you will also need a Spark installation. You can get Spark releases at
https://spark.apache.org/downloads.html.

Livy requires at least Spark 1.6 and supports both Scala 2.10 and 2.11 builds of Spark, Livy
will automatically pick repl dependencies through detecting the Scala version of Spark.

Livy also supports Spark 2.0+ for both interactive and batch submission, you could seamlessly
switch to different versions of Spark through ``SPARK_HOME`` configuration, without needing to
rebuild Livy.


Building Livy
=============

Livy is built using `Apache Maven`_. To check out and build Livy, run:

.. code:: shell

    git clone https://github.com/cloudera/livy.git
    cd livy
    mvn package

By default Livy is built against Apache Spark 1.6.2, but the version of Spark used when running
Livy does not need to match the version used to build Livy. Livy internally uses reflection to
mitigate the gaps between different Spark versions, also Livy package itself does not
contain a Spark distribution, so it will work with any supported version of Spark (Spark 1.6+)
without needing to rebuild against specific version of Spark.

.. _Apache Maven: http://maven.apache.org


Running Livy
============

In order to run Livy with local sessions, first export these variables:

.. code:: shell

   export SPARK_HOME=/usr/lib/spark
   export HADOOP_CONF_DIR=/etc/hadoop/conf

Then start the server with:

.. code:: shell

    ./bin/livy-server

Livy uses the Spark configuration under ``SPARK_HOME`` by default. You can override the Spark configuration
by setting the ``SPARK_CONF_DIR`` environment variable before starting Livy.

It is strongly recommended to configure Spark to submit applications in YARN cluster mode.
That makes sure that user sessions have their resources properly accounted for in the YARN cluster,
and that the host running the Livy server doesn't become overloaded when multiple user sessions are
running.


Livy Configuration
==================

Livy uses a few configuration files under configuration the directory, which by default is the
``conf`` directory under the Livy installation. An alternative configuration directory can be
provided by setting the ``LIVY_CONF_DIR`` environment variable when starting Livy.

The configuration files used by Livy are:

* ``livy.conf``: contains the server configuration. The Livy distribution ships with a default
  configuration file listing available configuration keys and their default values.

* ``spark-blacklist.conf``: list Spark configuration options that users are not allowed to override.
  These options will be restricted to either their default values, or the values set in the Spark
  configuration used by Livy.

* ``log4j.properties``: configuration for Livy logging. Defines log levels and where log messages
  will be written to. The default configuration will print log messages to stderr.

Upgrade from Livy 0.1
=====================

A few things changed between since Livy 0.1 that require manual intervention when upgrading.

- Sessions that were active when the Livy 0.1 server was stopped may need to be killed
  manually. Use the tools from your cluster manager to achieve that (for example, the
  ``yarn`` command line tool).

- The configuration file has been renamed from ``livy-defaults.conf`` to ``livy.conf``.

- A few configuration values do not have any effect anymore. Notably:

  * ``livy.server.session.factory``: this config option has been replaced by the Spark
    configuration under ``SPARK_HOME``. If you wish to use a different Spark configuration
    for Livy, you can set ``SPARK_CONF_DIR`` in Livy's environment. To define the default
    file system root for sessions, set ``HADOOP_CONF_DIR`` to point at the Hadoop configuration
    to use. The default Hadoop file system will be used.

  * ``livy.yarn.jar``: this config has been replaced by separate configs listing specific
    archives for different Livy features. Refer to the default ``livy.conf`` file shipped
    with Livy for instructions.

  * ``livy.server.spark-submit``: replaced by the ``SPARK_HOME`` environment variable.


Using the Programmatic API
==========================

Livy provides a programmatic Java/Scala and Python API that allows applications to run code inside
Spark without having to maintain a local Spark context. Here shows how to use the Java API.

Add the Cloudera repository to your application's POM:

.. code:: xml

    <repositories>
      <repository>
        <id>cloudera.repo</id>
        <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
        <name>Cloudera Repositories</name>
        <snapshots>
          <enabled>false</enabled>
        </snapshots>
      </repository>
    </repositories>


And add the Livy client dependency:

.. code:: xml

    <dependency>
      <groupId>com.cloudera.livy</groupId>
      <artifactId>livy-client-http</artifactId>
      <version>0.2.0</version>
    </dependency>


To be able to compile code that uses Spark APIs, also add the correspondent Spark dependencies.

To run Spark jobs within your applications, extend ``com.cloudera.livy.Job`` and implement
the functionality you need. Here's an example job that calculates an approximate value for Pi:

.. code:: java

    import java.util.*;

    import org.apache.spark.api.java.*;
    import org.apache.spark.api.java.function.*;

    import com.cloudera.livy.*;

    public class PiJob implements Job<Double>, Function<Integer, Integer>,
      Function2<Integer, Integer, Integer> {

      private final int samples;

      public PiJob(int samples) {
        this.samples = samples;
      }

      @Override
      public Double call(JobContext ctx) throws Exception {
        List<Integer> sampleList = new ArrayList<Integer>();
        for (int i = 0; i < samples; i++) {
          sampleList.add(i + 1);
        }

        return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
      }

      @Override
      public Integer call(Integer v1) {
        double x = Math.random();
        double y = Math.random();
        return (x*x + y*y < 1) ? 1 : 0;
      }

      @Override
      public Integer call(Integer v1, Integer v2) {
        return v1 + v2;
      }

    }


To submit this code using Livy, create a LivyClient instance and upload your application code to
the Spark context. Here's an example of code that submits the above job and prints the computed
value:

.. code:: java

    LivyClient client = new LivyClientBuilder()
      .setURI(new URI(livyUrl))
      .build();

    try {
      System.err.printf("Uploading %s to the Spark context...\n", piJar);
      client.uploadJar(new File(piJar)).get();

      System.err.printf("Running PiJob with %d samples...\n", samples);
      double pi = client.submit(new PiJob(samples)).get();

      System.out.println("Pi is roughly: " + pi);
    } finally {
      client.stop(true);
    }


To learn about all the functionality available to applications, read the javadoc documentation for
the classes under the ``api`` module.


Spark Example
=============

Here's a step-by-step example of interacting with Livy in Python with the `Requests`_ library. By
default Livy runs on port 8998 (which can be changed with the ``livy.server.port`` config option).
We’ll start off with a Spark session that takes Scala code:

.. code:: shell

    sudo pip install requests

.. code:: python

    import json, pprint, requests, textwrap
    host = 'http://localhost:8998'
    data = {'kind': 'spark'}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    r.json()

    {u'state': u'starting', u'id': 0, u'kind': u'spark'}

Once the session has completed starting up, it transitions to the idle state:

.. code:: python

    session_url = host + r.headers['location']
    r = requests.get(session_url, headers=headers)
    r.json()

    {u'state': u'idle', u'id': 0, u'kind': u'spark'}

Now we can execute Scala by passing in a simple JSON command:

.. code:: python

    statements_url = session_url + '/statements'
    data = {'code': '1 + 1'}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    r.json()

    {u'output': None, u'state': u'running', u'id': 0}

If a statement takes longer than a few milliseconds to execute, Livy returns
early and provides a statement URL that can be polled until it is complete:

.. code:: python

    statement_url = host + r.headers['location']
    r = requests.get(statement_url, headers=headers)
    pprint.pprint(r.json())

    {u'id': 0,
      u'output': {u'data': {u'text/plain': u'res0: Int = 2'},
                  u'execution_count': 0,
                  u'status': u'ok'},
      u'state': u'available'}

That was a pretty simple example. More interesting is using Spark to estimate
Pi. This is from the `Spark Examples`_:

.. code:: python

    data = {
      'code': textwrap.dedent("""
        val NUM_SAMPLES = 100000;
        val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
          val x = Math.random();
          val y = Math.random();
          if (x*x + y*y < 1) 1 else 0
        }.reduce(_ + _);
        println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
        """)
    }

    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    pprint.pprint(r.json())

    statement_url = host + r.headers['location']
    r = requests.get(statement_url, headers=headers)
    pprint.pprint(r.json())

    {u'id': 1,
     u'output': {u'data': {u'text/plain': u'Pi is roughly 3.14004\nNUM_SAMPLES: Int = 100000\ncount: Int = 78501'},
                 u'execution_count': 1,
                 u'status': u'ok'},
     u'state': u'available'}

Finally, close the session:

.. code:: python

    session_url = 'http://localhost:8998/sessions/0'
    requests.delete(session_url, headers=headers)

    <Response [204]>

.. _Requests: http://docs.python-requests.org/en/latest/
.. _Spark Examples: https://spark.apache.org/examples.html


PySpark Example
===============

PySpark has the same API, just with a different initial request:

.. code:: python

    data = {'kind': 'pyspark'}
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    r.json()

    {u'id': 1, u'state': u'idle'}

The Pi example from before then can be run as:

.. code:: python

    data = {
      'code': textwrap.dedent("""
        import random
        NUM_SAMPLES = 100000
        def sample(p):
          x, y = random.random(), random.random()
          return 1 if x*x + y*y < 1 else 0

        count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
        print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)
        """)
    }

    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    pprint.pprint(r.json())

    {u'id': 12,
    u'output': {u'data': {u'text/plain': u'Pi is roughly 3.136000'},
                u'execution_count': 12,
                u'status': u'ok'},
    u'state': u'running'}


SparkR Example
==============

SparkR has the same API:

.. code:: python

    data = {'kind': 'sparkr'}
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    r.json()

    {u'id': 1, u'state': u'idle'}

The Pi example from before then can be run as:

.. code:: python

    data = {
      'code': textwrap.dedent("""
        n <- 100000
        piFunc <- function(elem) {
          rands <- runif(n = 2, min = -1, max = 1)
          val <- ifelse((rands[1]^2 + rands[2]^2) < 1, 1.0, 0.0)
          val
        }
        piFuncVec <- function(elems) {
          message(length(elems))
          rands1 <- runif(n = length(elems), min = -1, max = 1)
          rands2 <- runif(n = length(elems), min = -1, max = 1)
          val <- ifelse((rands1^2 + rands2^2) < 1, 1.0, 0.0)
          sum(val)
        }
        rdd <- parallelize(sc, 1:n, slices)
        count <- reduce(lapplyPartition(rdd, piFuncVec), sum)
        cat("Pi is roughly", 4.0 * count / n, "\n")
        """)
    }

    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    pprint.pprint(r.json())

    {u'id': 12,
     u'output': {u'data': {u'text/plain': u'Pi is roughly 3.136000'},
                 u'execution_count': 12,
                 u'status': u'ok'},
     u'state': u'running'}


Community
=========

 * User group: http://groups.google.com/a/cloudera.org/group/livy-user
 * Dev group: http://groups.google.com/a/cloudera.org/group/livy-dev
 * Dev slack: https://livy-dev.slack.com.

   To join: http://livy-slack-invite.azurewebsites.net. Invite token: ``I'm not a bot``.
 * JIRA: https://issues.cloudera.org/browse/LIVY
 * Pull requests: https://github.com/cloudera/livy/pulls


REST API
========

GET /sessions
-------------

Returns all the active interactive sessions.

Request Parameters
^^^^^^^^^^^^^^^^^^

+------+-----------------------------------+------+
| name | description                       | type |
+======+===================================+======+
| from | The start index to fetch sessions | int  |
+------+-----------------------------------+------+
| size | Number of sessions to fetch       | int  |
+------+-----------------------------------+------+

Response Body
^^^^^^^^^^^^^

+----------+-------------------------------------+------+
| name     | description                         | type |
+==========+=====================================+======+
| from     | The start index of fetched sessions | int  |
+----------+-------------------------------------+------+
| total    | Number of sessions fetched          | int  |
+----------+-------------------------------------+------+
| sessions | `Session`_ list                     | list |
+----------+-------------------------------------+------+


POST /sessions
--------------

Creates a new interactive Scala, Python, or R shell in the cluster.

Request Body
^^^^^^^^^^^^

+--------------------------+------------------------------------------------+-----------------+
| name                     | description                                    | type            |
+==========================+================================================+=================+
| kind                     | The session kind (required)                    | `session kind`_ |
+--------------------------+------------------------------------------------+-----------------+
| proxyUser                | User to impersonate when starting the session  | string          |
+--------------------------+------------------------------------------------+-----------------+
| jars                     | jars to be used in this session                | List of string  |
+--------------------------+------------------------------------------------+-----------------+
| pyFiles                  | Python files to be used in this session        | List of string  |
+--------------------------+------------------------------------------------+-----------------+
| files                    | files to be used in this session               | List of string  |
+--------------------------+------------------------------------------------+-----------------+
| driverMemory             | Amount of memory to use for the driver process | string          |
+--------------------------+------------------------------------------------+-----------------+
| driverCores              | Number of cores to use for the driver process  | int             |
+--------------------------+------------------------------------------------+-----------------+
| executorMemory           | Amount of memory to use per executor process   | string          |
+--------------------------+------------------------------------------------+-----------------+
| executorCores            | Number of cores to use for each executor       | int             |
+--------------------------+------------------------------------------------+-----------------+
| numExecutors             | Number of executors to launch for this session | int             |
+--------------------------+------------------------------------------------+-----------------+
| archives                 | Archives to be used in this session            | List of string  |
+--------------------------+------------------------------------------------+-----------------+
| queue                    | The name of the YARN queue to which submitted  | string          |
+--------------------------+------------------------------------------------+-----------------+
| name                     | The name of this session                       | string          |
+--------------------------+------------------------------------------------+-----------------+
| conf                     | Spark configuration properties                 | Map of key=val  |
+--------------------------+------------------------------------------------+-----------------+
| heartbeatTimeoutInSecond | Timeout in second to which session be orphaned | int             |
+--------------------------+------------------------------------------------+-----------------+


Response Body
^^^^^^^^^^^^^

The created `Session`_.


GET /sessions/{sessionId}
-------------------------

Returns the session information.

Response Body
^^^^^^^^^^^^^

The `Session`_.


GET /sessions/{sessionId}/state
-------------------------------

Returns the state of session

Response
^^^^^^^^

+-------+-----------------------------------+--------+
| name  | description                       | type   |
+=======+===================================+========+
| id    | Session id                        | int    |
+-------+-----------------------------------+--------+
| state | The current state of session      | string |
+-------+-----------------------------------+--------+


DELETE /sessions/{sessionId}
----------------------------

Kills the `Session`_ job.


GET /sessions/{sessionId}/log
------------------------------

Gets the log lines from this session.

Request Parameters
^^^^^^^^^^^^^^^^^^

+------+-----------------------------------+------+
| name | description                       | type |
+======+===================================+======+
| from | Offset                            | int  |
+------+-----------------------------------+------+
| size | Max number of log lines to return | int  |
+------+-----------------------------------+------+

Response Body
^^^^^^^^^^^^^

+------+--------------------------+-----------------+
| name | description              | type            |
+======+==========================+=================+
| id   | The session id           | int             |
+------+--------------------------+-----------------+
| from | Offset from start of log | int             |
+------+--------------------------+-----------------+
| size | Number of log lines      | int             |
+------+--------------------------+-----------------+
| log  | The log lines            | list of strings |
+------+--------------------------+-----------------+


GET /sessions/{sessionId}/statements
------------------------------------

Returns all the statements in a session.

Response Body
^^^^^^^^^^^^^

+------------+-------------------+------+
| name       | description       | type |
+============+===================+======+
| statements | `statement`_ list | list |
+------------+-------------------+------+


POST /sessions/{sessionId}/statements
-------------------------------------

Runs a statement in a session.

Request Body
^^^^^^^^^^^^

+------+---------------------+--------+
| name | description         | type   |
+======+=====================+========+
| code | The code to execute | string |
+------+---------------------+--------+

Response Body
^^^^^^^^^^^^^

The `statement`_ object.


GET /sessions/{sessionId}/statements/{statementId}
--------------------------------------------------

Returns a specified statement in a session.

Response Body
^^^^^^^^^^^^^

The `statement`_ object.


POST /sessions/{sessionId}/statements/{statementId}/cancel
----------------------------------------------------------

Cancel the specified statement in this session.

Response Body
^^^^^^^^^^^^^

+------+----------------------------+--------+
| name | description                | type   |
+======+============================+========+
| msg  | is always "cancelled"      | string |
+------+----------------------------+--------+


GET /batches
-------------

Returns all the active batch sessions.

Request Parameters
^^^^^^^^^^^^^^^^^^

+------+-----------------------------------+------+
| name | description                       | type |
+======+===================================+======+
| from | The start index to fetch sessions | int  |
+------+-----------------------------------+------+
| size | Number of sessions to fetch       | int  |
+------+-----------------------------------+------+

Response Body
^^^^^^^^^^^^^

+----------+-------------------------------------+------+
| name     | description                         | type |
+==========+=====================================+======+
| from     | The start index of fetched sessions | int  |
+----------+-------------------------------------+------+
| total    | Number of sessions fetched          | int  |
+----------+-------------------------------------+------+
| sessions | `Batch`_ list                       | list |
+----------+-------------------------------------+------+


POST /batches
-------------

Request Body
^^^^^^^^^^^^

+----------------+---------------------------------------------------+-----------------+
| name           | description                                       | type            |
+================+===================================================+=================+
| file           | File containing the application to execute        | path (required) |
+----------------+---------------------------------------------------+-----------------+
| proxyUser      | User to impersonate when running the job          | string          |
+----------------+---------------------------------------------------+-----------------+
| className      | Application Java/Spark main class                 | string          |
+----------------+---------------------------------------------------+-----------------+
| args           | Command line arguments for the application        | list of strings |
+----------------+---------------------------------------------------+-----------------+
| jars           | jars to be used in this session                   | List of string  |
+----------------+---------------------------------------------------+-----------------+
| pyFiles        | Python files to be used in this session           | List of string  |
+----------------+---------------------------------------------------+-----------------+
| files          | files to be used in this session                  | List of string  |
+----------------+---------------------------------------------------+-----------------+
| driverMemory   | Amount of memory to use for the driver process    | string          |
+----------------+---------------------------------------------------+-----------------+
| driverCores    | Number of cores to use for the driver process     | int             |
+----------------+---------------------------------------------------+-----------------+
| executorMemory | Amount of memory to use per executor process      | string          |
+----------------+---------------------------------------------------+-----------------+
| executorCores  | Number of cores to use for each executor          | int             |
+----------------+---------------------------------------------------+-----------------+
| numExecutors   | Number of executors to launch for this session    | int             |
+----------------+---------------------------------------------------+-----------------+
| archives       | Archives to be used in this session               | List of string  |
+----------------+---------------------------------------------------+-----------------+
| queue          | The name of the YARN queue to which submitted     | string          |
+----------------+---------------------------------------------------+-----------------+
| name           | The name of this session                          | string          |
+----------------+---------------------------------------------------+-----------------+
| conf           | Spark configuration properties                    | Map of key=val  |
+----------------+---------------------------------------------------+-----------------+

Response Body
^^^^^^^^^^^^^

The created `Batch`_ object.


GET /batches/{batchId}
----------------------

Returns the batch session information.

Response Body
^^^^^^^^^^^^^

The `Batch`_.


GET /batches/{batchId}/state
----------------------------

Returns the state of batch session

Response
^^^^^^^^

+-------+-----------------------------------+--------+
| name  | description                       | type   |
+=======+===================================+========+
| id    | Batch session id                  | int    |
+-------+-----------------------------------+--------+
| state | The current state of batch session| string |
+-------+-----------------------------------+--------+


DELETE /batches/{batchId}
-------------------------

Kills the `Batch`_ job.


GET /batches/{batchId}/log
--------------------------

Gets the log lines from this batch.

Request Parameters
^^^^^^^^^^^^^^^^^^

+------+-----------------------------------+------+
| name | description                       | type |
+======+===================================+======+
| from | Offset                            | int  |
+------+-----------------------------------+------+
| size | Max number of log lines to return | int  |
+------+-----------------------------------+------+

Response Body
^^^^^^^^^^^^^

+------+--------------------------+-----------------+
| name | description              | type            |
+======+==========================+=================+
| id   | The batch id             | int             |
+------+--------------------------+-----------------+
| from | Offset from start of log | int             |
+------+--------------------------+-----------------+
| size | Number of log lines      | int             |
+------+--------------------------+-----------------+
| log  | The log lines            | list of strings |
+------+--------------------------+-----------------+


REST Objects
============

Session
-------

A session represents an interactive shell.

+----------------+------------------------------------------+----------------------------+
| name           | description                              | type                       |
+================+==========================================+============================+
| id             | The session id                           | int                        |
+----------------+------------------------------------------+----------------------------+
| appId          | The application id of this session       | String                     |
+----------------+------------------------------------------+----------------------------+
| owner          | Remote user who submitted this session   | String                     |
+----------------+------------------------------------------+----------------------------+
| proxyUser      | User to impersonate when running         | String                     |
+----------------+------------------------------------------+----------------------------+
| kind           | Session kind (spark, pyspark, or sparkr) | `session kind`_            |
+----------------+------------------------------------------+----------------------------+
| log            | The log lines                            | list of strings            |
+----------------+------------------------------------------+----------------------------+
| state          | The session state                        | string                     |
+----------------+------------------------------------------+----------------------------+
| appInfo        | The detailed application info            | Map of key=val             |
+----------------+------------------------------------------+----------------------------+


Session State
^^^^^^^^^^^^^

+---------------+----------------------------------+
| value         | description                      |
+===============+==================================+
| not_started   | Session has not been started     |
+---------------+----------------------------------+
| starting      | Session is starting              |
+---------------+----------------------------------+
| idle          | Session is waiting for input     |
+---------------+----------------------------------+
| busy          | Session is executing a statement |
+---------------+----------------------------------+
| shutting_down | Session is shutting down         |
+---------------+----------------------------------+
| error         | Session errored out              |
+---------------+----------------------------------+
| dead          | Session has exited               |
+---------------+----------------------------------+
| success       | Session is successfully stopped  |
+---------------+----------------------------------+

Session Kind
^^^^^^^^^^^^

+-------------+------------------------------------+
| value       | description                        |
+=============+====================================+
| spark       | Interactive Scala  Spark session   |
+-------------+------------------------------------+
| `pyspark`_  | Interactive Python 2 Spark session |
+-------------+------------------------------------+
| `pyspark3`_ | Interactive Python 3 Spark session |
+-------------+------------------------------------+
| sparkr      | Interactive R Spark session        |
+-------------+------------------------------------+

pyspark
^^^^^^^
To change the Python executable the session uses, Livy reads the path from environment variable
``PYSPARK_PYTHON`` (Same as pyspark).

Like pyspark, if Livy is running in ``local`` mode, just set the environment variable.
If the session is running in ``yarn-cluster`` mode, please set
``spark.yarn.appMasterEnv.PYSPARK_PYTHON`` in SparkConf so the environment variable is passed to
the driver.

pyspark3
^^^^^^^^
To change the Python executable the session uses, Livy reads the path from environment variable
``PYSPARK3_PYTHON``.

Like pyspark, if Livy is running in ``local`` mode, just set the environment variable.
If the session is running in ``yarn-cluster`` mode, please set
``spark.yarn.appMasterEnv.PYSPARK3_PYTHON`` in SparkConf so the environment variable is passed to
the driver.

Statement
---------

A statement represents the result of an execution statement.

+--------+----------------------+---------------------+
| name   | description          | type                |
+========+======================+=====================+
| id     | The statement id     | integer             |
+--------+----------------------+---------------------+
| state  | The execution state  | statement state     |
+--------+----------------------+---------------------+
| output | The execution output | statement output    |
+--------+----------------------+---------------------+

Statement State
^^^^^^^^^^^^^^^

+------------+----------------------------------------------------+
| value      | description                                        |
+============+====================================================+
| waiting    | Statement is enqueued but execution hasn't started |
+------------+----------------------------------------------------+
| running    | Statement is currently running                     |
+------------+----------------------------------------------------+
| available  | Statement has a response ready                     |
+------------+----------------------------------------------------+
| error      | Statement failed                                   |
+------------+----------------------------------------------------+
| cancelling | Statement is being cancelling                      |
+------------+----------------------------------------------------+
| cancelled  | Statement is cancelled                             |
+------------+----------------------------------------------------+

Statement Output
^^^^^^^^^^^^^^^^

+-----------------+-------------------+----------------------------------+
| name            | description       | type                             |
+=================+===================+==================================+
| status          | Execution status  | string                           |
+-----------------+-------------------+----------------------------------+
| execution_count | A monotonically   | integer                          |
|                 | increasing number |                                  |
+-----------------+-------------------+----------------------------------+
| data            | Statement output  | An object mapping a mime type to |
|                 |                   | the result. If the mime type is  |
|                 |                   | ``application/json``, the value  |
|                 |                   | is a JSON value.                 |
+-----------------+-------------------+----------------------------------+

Batch
-----

+----------------+-------------------------------------+-----------------+
| name           | description                         | type            |
+================+=====================================+=================+
| id             | The session id                      | int             |
+----------------+-------------------------------------+-----------------+
| appId          | The application id of this session  | String          |
+----------------+-------------------------------------+-----------------+
| appInfo        | The detailed application info       | Map of key=val  |
+----------------+-------------------------------------+-----------------+
| log            | The log lines                       | list of strings |
+----------------+-------------------------------------+-----------------+
| state          | The batch state                     | string          |
+----------------+-------------------------------------+-----------------+


License
=======

Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
