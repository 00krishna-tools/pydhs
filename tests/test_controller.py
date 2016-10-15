#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_controller
----------------------------------

Tests for `controller` module.
"""

import pytest
import psycopg2
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.DbTable import Database_Table
from pydhs.Controller import Controller
from contextlib import contextmanager
from click.testing import CliRunner
import pandas as pd
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
    conn = DatabaseSqlalchemy('db_antonio_india',
                             'krishnab',
                             '3kl4vx71',
                             'localhost',
                             5432)

    return(conn)




class TestController(object):
    @classmethod
    def setup_class(cls):
        pass

# '''
# Test Controller Database Access
# -------------------------------
#
# This first section tests the ability of the controller to connect to the
# database. The next section tests basic database functionality.
#
#
# '''
#     def test_controller_connection(self):
#         d = Controller('db_antonio_india')
#         # print(type(d.db.conn))
#         assert isinstance(d.db.conn, psycopg2.extensions.connection)

    def test_sqlalchemy_get_table_names_as_dataframe(self, dbsqlalchemy):
        res = dbsqlalchemy.get_table_list_as_dataframe('public')
        # print(res)
        assert isinstance(res, pd.DataFrame)

    def test_sqlalchemy_write_table_names_to_csv(self):
        c = Controller('db_antonio_india')
        c.action_write_table_list_to_csv()

    def test_sqlalchemy_get_variables_from_each_table(self):
        c = Controller('db_antonio_india')
        res = c.action_get_variable_names_for_each_table_in_database(
            'tablelist2.csv')
        assert isinstance(res, dict)

    def test_intersection_of_columns(self):
        c = Controller('db_antonio_india')
        c.action_get_intersection_of_fields_across_database_tables(
            'tablelist2.csv')

