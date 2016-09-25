"""
Database Class
-----------------

This class captures information and methods on the main dhs database. This
class will create both a regular psycopg2 connection to the database for
running queries, but also create a pandas connection to the postgres database with sqlalchemy to write tables.


"""

__author__ = 'krishnab'
__version__ = '0.1.0'

import numpy as np
import pandas as pd
import os
import datetime
import psycopg2 as pg
import psycopg2.extras as pgextras
import pandas.io.sql as psql
from sqlalchemy import create_engine


## Initialize Constants




class Database():


    def __init__(self,
                 dbname,
                 username,
                 password,
                 hostname = 'localhost',
                 portnumber = 5432):


    ## Setup psycopg2 database connection.

        self.conn = self.connect_to_postgres_through_psycopg2(dbname,
                                                              username,
                                                              password,
                                                              hostname,
                                                              portnumber)



        if isinstance(self.conn,pg.extensions.connection):

            self.rcursor = self.conn.cursor()
            self.dictcursor = self.conn.cursor(cursor_factory=pgextras.DictCursor)




    ## Setup pandas and sqlalchemy connection to database


    def connect_to_postgres_through_psycopg2(self,
                                             dbname,
                                             username,
                                             password,
                                             hostname = 'localhost',
                                             portnumber = 5432):

        try:

            connection_string = "dbname=%s user=%s host=%s password=%s " \
                                "port=%d " % (dbname,
                                     username,
                                     password,
                                     hostname,
                                     int(portnumber))

            conn = pg.connect(connection_string)
        except:

            print('check database connection information before proceeding')
            raise

        return(conn)


    def connect_to_postgres_through_sqlalchemy(self,
                                               username,
                                               password,
                                               dbname,
                                               hostname,
                                               portnumber):

        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(username, password, hostname, portnumber, dbname)

        # The return value of create_engine() is our connection object
        con = sqlalchemy.create_engine(url, client_encoding='utf8')

        # We then bind the connection to MetaData()
        meta = sqlalchemy.MetaData(bind=con, reflect=True)

        return con, meta



    def get_dictionary_cursor_query(self,
                                    query,
                                    strings = ('',)):

        try:
            self.dictcursor.execute(query, strings)

        except:
            print('Sorry, something went wrong with running the query')
            raise

        return (self.dictcursor.fetchall())


    def get_regular_cursor_query(self,
                                 query,
                                 strings = ('',)):

        try:
            self.rcursor.execute(query, strings)

        except:
            print('Sorry, something went wrong with running the query')
            raise

        return (self.rcursor.fetchall())



    def get_table_column_names(self,
                               tablename):



        query = "SELECT column_name FROM information_schema.columns WHERE " \
                "table_name=(%s)"

        return (self.get_dictionary_cursor_query(query, (tablename,)))



    def get_list_of_tables_in_database(self,
                                       schema):

        query = "SELECT tablename FROM pg_catalog.pg_tables where schemaname " \
                "=(%s)"

        return (self.get_dictionary_cursor_query(query, (schema,)))





    def set_connection_closed(self):

        self.conn.commit()

        self.rcursor.close()
        self.dictcursor.close()
        self.conn.close()

        return(1)





