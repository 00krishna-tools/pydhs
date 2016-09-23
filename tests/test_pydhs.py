#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_pydhs
----------------------------------

Tests for `pydhs` module.
"""

import pytest
import psycopg2
from pydhs.Database import Database
from pydhs.DbTable import Database_Table
from pydhs.Controller import Controller
from contextlib import contextmanager
from click.testing import CliRunner

from pydhs import pydhs
from pydhs import cli


class TestPydhs(object):
    @classmethod
    def setup_class(cls):
        pass


    def test_database_get_regular_cursor_query(self):
        d = Database('db_antonio_india',
                     'krishnab',
                     'localhost',
                     '3kl4vx71',
                     5432)
        res = d.get_regular_cursor_query("""select column_name from
        information_schema.columns where table_name = 'iabr42fl'""")
        # print(res)
        # print(type(res))
        assert isinstance(res, list)


    def test_database_get_dictionary_cursor_query(self):
        d = Database('db_antonio_india',
                     'krishnab',
                     'localhost',
                     '3kl4vx71',
                     5432)
        res = d.get_dictionary_cursor_query(
            """select column_name from information_schema.columns where table_name = 'iabr42fl'""")
        # print(res)
        # print(type(res))
        assert isinstance(res, list)




    def test_database_string_query(self):
        d = Database('db_antonio_india',
                     'krishnab',
                     'localhost',
                     '3kl4vx71',
                     5432)
        # print(type(d.db.conn))

        res = d.get_table_column_names('iabr23fl')
        #print(res)
        assert isinstance(res, list)


    def test_database_get_table_names(self):
        d = Database('db_antonio_india',
                     'krishnab',
                     'localhost',
                     '3kl4vx71',
                     5432)

        res = d.get_list_of_tables_in_database('public')
        print(res)
        assert isinstance(res, list)

## Controller test cases




    def test_controller_connection(self):
        d = Controller('db_antonio_india')
        # print(type(d.db.conn))
        assert isinstance(d.db.conn, psycopg2.extensions.connection)
