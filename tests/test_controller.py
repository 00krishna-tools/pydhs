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
from pydhs.DbTable import Database_Table
from pydhs.Controller import Controller
from contextlib import contextmanager
from click.testing import CliRunner

from pydhs import pydhs
from pydhs import cli


class TestController(object):
    @classmethod
    def setup_class(cls):
        pass

'''
Test Controller Database Access
-------------------------------

This first section tests the ability of the controller to connect to the
database. The next section tests basic database functionality.


'''
#     def test_controller_connection(self):
#         d = Controller('db_antonio_india')
#         # print(type(d.db.conn))
#         assert isinstance(d.db.conn, psycopg2.extensions.connection)
