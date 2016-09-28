#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test Database Module
----------------------------------

Tests for the Database Module
"""

import pytest
import psycopg2
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
import sqlalchemy
from pydhs.Database import DatabaseAsyncpg
from pydhs.DbTable import Database_Table
from pydhs.Controller import Controller
from contextlib import contextmanager
from click.testing import CliRunner

from pydhs import pydhs
from pydhs import cli





@pytest.fixture(scope="module")
def dbpsycopg2():
    conn = DatabasePsycopg2('db_antonio_india',
                             'krishnab',
                             '3kl4vx71',
                             'localhost',
                             5432)

    return(conn)

@pytest.fixture(scope="module")
def dbsqlalchemy():
    conn = DatabaseSqlalchemy('krishnab',
                             '3kl4vx71',
                             'db_antonio_india',
                             'localhost',
                             5432)

    return(conn)


'''
Testing Database Connections
----------------------------
First test the different database connection types. There are 3 types of
database connections used in this project or module: psycopg2, asynpg,
and sqlalchemy. These are all needed because they perform different tasks.

psycopg2 is the main workhorse for communicating data to the db. sqlalchemy
with pandas is used for writing tables to the database, because the other
packages don't make writing very nice. And asynpg is used for speed. Asyncpg
is the least mature, so I will use it, but also need to go to psycopg2 if
necessary.


'''


class TestPydhs(object):
    @classmethod
    def setup_class(cls):
        pass

    def test_psycopg2_connection(self):

        dt = DatabasePsycopg2('db_antonio_india',
                             'krishnab',
                             '3kl4vx71',
                             'localhost',
                             5432)

        assert isinstance(dt.conn, psycopg2.extensions.connection)
        dt.set_connection_closed()

    def test_sqlalchemy_connection(self):

        dt = DatabaseSqlalchemy(
                             'krishnab',
                             '3kl4vx71',
                             'db_antonio_india',
                             'localhost',
                             5432)


        #print(type(dt.conn))
        assert isinstance(dt.conn, sqlalchemy.engine.base.Engine)

    def test_asyncpg_connection(self):
        pass











'''
Testing Database Cursors
------------------------

Now that the database connections themselves are valid, I need to test the
different cursor types. I create and check each cursor type for each type of
connection.

psycopg2 has 2 types of cursors: regular(list) and dictionary.

sqlalchemy has XXX cursors

asyncpg has XXX cursors


'''


class Test_Database_Cursors(object):
    @classmethod
    def setup_class(cls):
        pass


    def test_database_psycopg2_get_regular_cursor_query(self, dbpsycopg2):
        query = """select column_name from information_schema.columns where
        table_name = 'iabr42fl'"""
        res = dbpsycopg2.get_regular_cursor_query(query)
        # print(res)
        # print(type(res))
        assert isinstance(res, list)


    def test_database_psycopg2_get_dictionary_cursor_query(self, dbpsycopg2):
        query = """select column_name from information_schema.columns where
        table_name = 'iabr42fl'"""
        res = dbpsycopg2.get_dictionary_cursor_query(query)
        # print(res)
        # print(type(res))
        assert isinstance(res, list)


    def test_database_get_table_names(self, dbpsycopg2):

        res = dbpsycopg2.get_list_of_tables_in_database('public')
        #print(res)
        assert isinstance(res, list)


    def test_database_string_query(self, dbpsycopg2):


        res = dbpsycopg2.get_table_column_names('iabr23fl')
        print(res)
        assert isinstance(res, list)
