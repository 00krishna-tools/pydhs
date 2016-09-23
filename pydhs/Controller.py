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
import pydhs.Database as DBase
#import pydhs.DbTable as DTable


## Initialize Constants




class Controller():

    def __init__(self, dbname):


    ## create a database object inside the controller to manage state changes
    #  to the database.

        self.db = DBase.Database(dbname,
                                    'krishnab',
                                    'localhost',
                                    '3kl4vx71',
                                    5432)



    def get_table_columns(self, tablename):

        return(self.db.get_table_columns_dict(tablename))






    def action_write_table_list_to_database(self):



        ## This function will create a dictionary of all column names in the
        # database and then intersect them and union them.


        # Get list of tables to iterate over

        table_list = self.db.get_list_of_tables_in_database('public')


        for



    def write_variable_names_to_database(self):

        pass

def get_intersection_of_dictionary(query_dictionary):

    return(reduce(set.intersection,
                   (set(val) for val in query_dictionary.values())))


def get_union_of_dictionary():

    return(reduce(set.union,
                  (set(val) for val in query_dictionary.values())))


