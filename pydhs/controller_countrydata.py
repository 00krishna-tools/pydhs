"""
Controller Class
-----------------

This class contains the controller logic for the application. This takes
input from the table and other interface objects and then manages change of
state for the database. It will also pass the state changes to the table
objects.

"""

__author__ = 'krishnab'
__version__ = '0.1.0'

import numpy as np
import pandas as pd
import os
import datetime
import psycopg2 as pg
#import pydhs.DbTable as DTable
import psycopg2
from psycopg2 import sql
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
import sqlalchemy
from functools import reduce
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.extensions import AsIs
from pydhs.Data_Cleaning_Constants import CLEAN_DHS_YEARS


## Initialize Constants

TABLENAMES = ["union_table", "intersection_table"]


class Controller():

    def __init__(self, dbname):

    ## create a database object inside the controller to manage state changes
    #  to the database.

        self.db = DatabasePsycopg2(dbname,
                                    'krishnab',
                                    '3kl4vx71',
                                    'localhost',
                                    5433)


        self.conn_sqlalchemy = DatabaseSqlalchemy(dbname,
                                                  'krishnab',
                                                  '3kl4vx71',
                                                  'localhost',
                                                  5433)

        self.database_table_fields = {}

    def update_country_data_ihme_table_year_field(self):

        query = """ALTER TABLE IF EXISTS country_ihme RENAME year TO year_ihme;"""

        self.db.get_regular_cursor_query_no_return(query)

        print('column name in ihme updated')

    def create_table_country_data_from_joining_data_sources(self):

        query = """CREATE TABLE country_data AS
                    SELECT *
                    FROM country_bj_regime
                    LEFT JOIN country_ihme ON country_bj_regime.countryisocode = country_ihme.iso3
                    AND country_bj_regime.year = country_ihme.year_ihme;"""

        self.db.get_regular_cursor_query_no_return(query)

        print('new table country_data created.')

    def get_country_column_names_and_add_to_intersection_table(self):

        columnames = self.db.get_table_column_names('country_data')

        for n in columnames:
            self.db.add_column_to_table('intersection_table_birth', n)

