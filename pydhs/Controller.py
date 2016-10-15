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
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
import sqlalchemy
from functools import reduce


## Initialize Constants




class Controller():

    def __init__(self, dbname):


    ## create a database object inside the controller to manage state changes
    #  to the database.

        self.db = DatabasePsycopg2(dbname,
                                    'krishnab',
                                    '3kl4vx71',
                                    'localhost',
                                    5432)


        self.conn_sqlalchemy = DatabaseSqlalchemy(dbname,
                                                  'krishnab',
                                                  '3kl4vx71',
                                                  'localhost',
                                                  5432)

    def _get_table_columns(self, tablename):

        return(self.db.get_table_columns_dict(tablename))


    def action_write_table_list_to_csv(self):



        ## This function will create a dictionary of all column names in the
        # database and then intersect them and union them.


        # Get list of tables to iterate over

        table_list = self.conn_sqlalchemy.get_table_list_as_dataframe('public')
        table_list.to_csv('tablelist.csv')



    def action_get_variable_names_for_each_table_in_database(self, tablefile):

        tables = pd.read_csv(tablefile)
        #print(tables.columns.values)

        # create an empty dictionary to hold the info.

        table_fields = {}

        ## iterate over tables and

        for tbl in tables['tablename']:
            table_fields[tbl] = set(self.db.get_table_column_names(tbl))

        print(table_fields)
        return(table_fields)


    def action_get_intersection_of_fields_across_database_tables(self,
                                                                 tablefile):

        tables = self.action_get_variable_names_for_each_table_in_database(
            tablefile)

        fields = [value for key, value in tables.items()]

        intersected_columns = self.get_intersection_of_setlist(fields)

        pd.DataFrame(list(intersected_columns)).to_csv(
            'intersection_fields.csv')
        return(list(intersected_columns))


    def action_get_union_of_fields_across_database_tables(self,tablefile):

        tables = self.action_get_variable_names_for_each_table_in_database(
            tablefile)

        fields = [value for key, value in tables.items()]

        union_columns = self.get_union_of_setlist(fields)

        pd.DataFrame(list(union_columns)).to_csv('union_fields.csv')
        return(list(union_columns))


    def get_intersection_of_setlist(self,setlist):

        return(set.intersection(*setlist))


    def get_union_of_setlist(self, setlist):

        return (set.union(*setlist))


