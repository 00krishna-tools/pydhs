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
import psycopg2.extras as pgextras

## Initialize Constants




class Database():
    def __init__(self, dbname, username, hostname, password, portnumber):

        try:
            self.conn = self.get_connection(dbname,
                                            username,
                                            hostname,
                                            password,
                                            portnumber)

        except:
            print('check database connection information before proceeding')

        if self.conn is pg.extensions.connection:

            self.cursor = self.conn.cursor()
            self.dictcursor = self.conn.cursor(cursor_factory=pgextras.DictCursor)


    def get_connection(self,
                       dbname,
                       username,
                       hostname,
                       password,
                       portnumber):


        try:
            conn = pg.connect(
                "dbname={0} user={1} host={2} "
                "password={3} port={4} ".format(str(dbname),
                                                str(username),
                                                str(hostname),
                                                str(password),
                                                int(portnumber)))
            return(conn)
        except:
            print
            "I am unable to connect to the database"
            return (1)




    def get_dictionary_cursor_query(self,query):

        return(self.dictcursor.execute(query))


    def get_regular_cursor(self, query):

        return(self.cursor.execute(query))


    def get_table_columns_dict(self, tablename):

        query = """SELECT column_name FROM information_schema.columns WHERE
        table_name={0}""".format(str(tablename))

        return(self.get_dictionary_cursor_query(query))

    def set_connection_closed(self):
        pass





