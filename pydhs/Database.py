"""
Database Class
-----------------

This class captures information and methods on the main dhs database.

"""

__author__ = 'krishnab'
__version__ = '0.1.0'

import numpy as np
import pandas as pd
import os
import datetime
import psycopg2 as pg


## Initialize Constants




class Database():
    def __init__(self, dbname):

        try:
            self.conn = self.get_connection(dbname)

        except:
            print('check database connection information before proceeding')

        if self.conn is pg.extensions.connection:

            self.cursor = self.conn.cursor()
            self.dictcursor = self.conn.cursor(cursor_factory=pg.extras.DictCursor)


    def get_connection(self, dbname):


        try:
            conn = pg.connect(
                "dbname={0} user='krishnab' host='localhost' "
                "password='3kl4vx71' port=5432 ".format(str(dbname)))
            return(conn)
        except:
            print
            "I am unable to connect to the database"
            return (1)




    def get_table_columns(self,tablename):

        return(self.conn.cursor().execute("""select column_name from information_schema.columns where
                table_name={0}""".format(str(tablename))))


    def get_tables_list(self):

        cursor = self.conn.cursor(cursor_factory=pg.extras.DictCursor)





    def set_connection_closed(self):
        pass





