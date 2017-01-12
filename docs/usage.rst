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
    blocks:
    - name: input
        type: amqp
        kwargs:
            server: '172.20.0.2'
    - name: output
        type: sql
        kwargs:
            url: 'postgres://postgres@172.20.0.3/database'
    flows:
    - - name: input
        kwargs:
            exchange_name: logs
        - name: output
        kwargs:
            query:
            INSERT INTO logs (message, message_vector)
            VALUES (:message, to_tsvector('english', :message))
    - - name: input
        kwargs:
            exchange_name: events
        - name: output
        kwargs:
            query:
            INSERT INTO events (message, message_vector)
            VALUES (:message, to_tsvector('english', :message))

where:
    - *size_limit*: batcher size limit
    - *time_limit*: batcher size limit
    - *blocks*: list of building blocks to use in the flows
    - *flows*: list of blocks connected to transfer information
      information


Blocks
======

A block rabbithole is the name of the little piece that can be added to a flow
to receive/send messages as needed to build the desired flow of information.
There are currently three different kinds of blocks:

    input

        an input block is a block that receives a messages from an external
        source, such as an amqp server, and transfers them as they are received
        to the next block in the flow.

    batchers

        rabbithole uses the concept of batchers that is also used in
        logstash_. A batcher is just an in-memory queue whose goal is to output
        data more efficiently by writing multiple messages at once.  It keeps
        messages in memory until its capacity has been filled up or until a
        time limit is exceeded. Both parameters can be set in the configuration
        file.

        Batchers are automatically added between blocks in a flow, so there's
        no need to include them explicitly in the configuration file.

    output

        an output block is a block that receives messages from the previous
        block and sends them to an external output such as a database.

Flow
====

A flow is a sequence of blocks that are connected to transfer information from
the initial input block to the final output one.

Available blocks
================

The following blocks are available in rabbithole.

amqp
----

ampq is an input flow that can receive data from amqp servers.

.. code-block:: yaml

    blocks:
    - name: input
        type: amqp
        kwargs:
            server: '172.20.0.2'
    flows:
    - - name: input
        kwargs:
            exchange_name: logs

where:

    *server*: is the IP address of the amq server to connect to
    *exchange_name* is the name of the exchange for which messages will be
    transferred in a given flow


sql
---

sql is an output flow that can write data to SQL databases.

.. code-block:: yaml

    blocks:
    - name: output
        type: sql
        kwargs:
            url: 'postgres://postgres@172.20.0.3/database'
    flows:
    - - name: output
        kwargs:
            query:
            INSERT INTO logs (message, message_vector)
            VALUES (:message, to_tsvector('english', :message))

where:

    *url* is the `connection string`_ to the database.
    *query* is the `query`_ to execute when a message is received in a given
    flow.

Note that the underlying implementation uses sqlalchemy_, so please refer to
its documentation for more information about their format.


.. _logstash: https://www.elastic.co/products/logstash
.. _connection string: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
.. _query: http://docs.sqlalchemy.org/en/latest/core/sqlelement.html?highlight=text#sqlalchemy.sql.expression.text
.. _sqlalchemy: http://www.sqlalchemy.org/
