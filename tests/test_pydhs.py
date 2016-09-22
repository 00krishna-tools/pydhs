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

    def test_database_connection(self):
        d = Controller('db_antonio_india')
        print(type(d.db.conn))
        assert isinstance(d.db.conn, psycopg2.extensions.connection)

    def test_controller_get_table_columns(self):

        d = Controller('db_antonio_india')
        res = d.get_table_columns('iabr42fl')
        print(res)
        assert isinstance(res, dict)

