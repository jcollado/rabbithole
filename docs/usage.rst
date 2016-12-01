=====
Usage
=====

CLI
===

Rabbit Hole is a command line tool that has been written as a lightweight
alternative to logstash_ for the specific use case in which the *input* is an
*amqp* server and the *output* is a SQL database.

It can be executed from the command line like this::

    $ rabbithole config.yml

where *config.yml* is a YAML configuration file. For example:

.. code-block:: yaml

    size_limit: 5
    time_limit: 15
    amqp: '172.20.0.2'
    sql: 'postgres://postgres@172.20.0.3/database'
    output:
      logs:
        INSERT INTO logs (message, message_vector)
        VALUES (:message, to_tsvector('english', :message))
      events:
        INSERT INTO events (message, message_vector)
        VALUES (:message, to_tsvector('english', :message))

where:
    - *size_limit*: batcher size limit
    - *time_limit*: batcher size limit
    - *amqp*: AMQP server address
    - *sql*: Database connection URL
    - *output*: Mapping from AMQP exchange names to SQL queries


Building blocks
===============

Input
-----

AMQP is assumed to be the messages input. What can be configured is the IP
address of the server and the exchanges for which messages should be delivered
specified as the keys of the *output* field.

Batchers
--------

Rabbit Hole uses the concept of batchers that is also used in logstash_. A
batcher is just an in-memory queue whose goal is to output data more
efficiently by writing multiple messages at once.

The batcher keeps messages in memory until its capacity has been filled up or
until a time limit is exceeded. Both parameters can be set in the configuration
file.

Output
------

A SQL database is assumed to be the messages output. What can be configured is
the `database URL`_ and the queries_ to execute for the messages received for
each exchange. Note that the underlying implementation uses sqlalchemy_, so
please refer to its documentation for more information about their format.


.. _logstash: https://www.elastic.co/products/logstash
.. _database URL: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
.. _queries: http://docs.sqlalchemy.org/en/latest/core/sqlelement.html?highlight=text#sqlalchemy.sql.expression.text
.. _sqlalchemy: http://www.sqlalchemy.org/
