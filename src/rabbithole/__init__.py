# -*- coding: utf-8 -*-

"""RabbitHole: Store messages from an AMQP into a SQL database.

The way the message are stores is that each exchange name is mapped to a SQL
query that is executed when neded.

"""

__author__ = """Javier Collado"""
__email__ = 'javier@gigaspaces.com'
__version__ = '0.3.0'
