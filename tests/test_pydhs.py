#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_pydhs
----------------------------------

Tests for `pydhs` module.
"""

import pytest
import psycopg2 as pg
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
        d = Database('db_antonio_india')
        assert isinstance(d.conn, pg.extensions.connection)


    def test_database_get_




    # def test_command_line_interface(self):
    #     runner = CliRunner()
    #     result = runner.invoke(cli.main)
    #     assert result.exit_code == 0
    #     assert 'pydhs.cli.main' in result.output
    #     help_result = runner.invoke(cli.main, ['--help'])
    #     assert help_result.exit_code == 0
    #     assert '--help  Show this message and exit.' in help_result.output
    #
    # @classmethod
    # def teardown_class(cls):
    #     pass

# def test_database_connection():
#
