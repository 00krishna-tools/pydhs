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
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData
import asyncio
import asyncpg
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer



## Initialize Constants





'''
DatabasePsycopg2 Class:
-----------------------

Psycopg2 is the main postgres access module. It is also the most mature. So
that is the basic workhorse of the Database access module. One limitation of
psycopg2 is that it does not do well for writing tables to postgres, and that is
why the DatabaseSqlalchemy class was made. Most queries go through psycopg2
if possible. DatabaseAsyncpg uses a newer faster module to communicate with
postgresql, but it is also the least mature.


'''


class DatabasePsycopg2():


    def __init__(self,
                 dbname,
                 username,
                 password,
                 hostname = 'localhost',
                 portnumber = 5433):


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
                                             portnumber = 5433):

        try:

            connection_string = "dbname=%s user=%s host=%s password=%s " \
                                "port=%d " % (dbname,
                                     username,
                                     hostname,
                                     password,
                                     int(portnumber))

            conn = pg.connect(connection_string)
        except:

            print('check database connection information before proceeding')
            raise

        return(conn)



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


    def get_regular_cursor_query_no_return(self,
                                 query,
                                 strings = ('',)):

        try:
            self.rcursor.execute(query, strings)
            self.conn.commit()
        except:
            print('Sorry, something went wrong with running the query')
            raise

        return (0)


    def get_table_column_names(self,
                               tablename):



        query = "SELECT column_name FROM information_schema.columns WHERE " \
                "table_name=(%s)"

        return (self.get_regular_cursor_query(query, (tablename,)))



    def get_list_of_tables_in_database(self,
                                       schema):

        query = "SELECT tablename FROM pg_catalog.pg_tables where schemaname " \
                "=(%s)"

        return (self.get_dictionary_cursor_query(query, (schema,)))

    def set_all_table_names_to_lowercase(self):

        # This query will generate queries for all capitalized tablenames"

        query = """SELECT 'ALTER TABLE ' || quote_ident(t.table_schema) || '.'
  || quote_ident(t.table_name) || ' RENAME TO ' || quote_ident(lower(t.table_name)) || ';' As ddlsql
  FROM information_schema.tables As t
  WHERE t.table_schema NOT IN('information_schema', 'pg_catalog')
      AND t.table_name <> lower(t.table_name)
  ORDER BY t.table_schema, t.table_name;"""

        list_of_updates = self.get_regular_cursor_query(query)

        print(list_of_updates)
        for q in list_of_updates:
            print(type(q[0]))
            print(q[0])
            self.get_regular_cursor_query_no_return(q[0])

        list_of_updates = self.get_regular_cursor_query(query)

        if len(list_of_updates) > 0:
            print("not all table names were fixed. Try again.")

    def set_all_field_names_to_lowercase(self):

        query = """SELECT  'ALTER TABLE ' || quote_ident(c.table_schema) || '.'
  || quote_ident(c.table_name) || ' RENAME "' || c.column_name || '" TO ' || quote_ident(lower(c.column_name)) || ';' As ddlsql
  FROM information_schema.columns As c
  WHERE c.table_schema NOT IN('information_schema', 'pg_catalog')
      AND c.column_name <> lower(c.column_name)
  ORDER BY c.table_schema, c.table_name, c.column_name;"""

        list_of_updates = self.get_regular_cursor_query(query)

        print(list_of_updates)
        for q in list_of_updates:
            print(type(q[0]))
            print(q[0])
            self.get_regular_cursor_query_no_return(q[0])

        list_of_updates = self.get_regular_cursor_query(query)

        if len(list_of_updates) > 0:
            print("not all table names were fixed. Try again.")


    def check_existence_or_drop_query(self, tablename):

        query = 'DROP TABLE IF EXISTS {};'.format(tablename)
        self.get_regular_cursor_query_no_return(query)

    def get_variables_by_table(self):

        vals = {'schemaname': 'public'}

        query = """SELECT table_catalog, table_schema, table_name, column_name, data_type, is_generated, is_updatable FROM information_schema.columns where table_schema ='public'"""

        return(pd.read_sql_query(query, self.conn))

    def set_connection_closed(self):

        self.conn.commit()

        self.rcursor.close()
        self.dictcursor.close()
        self.conn.close()

        return(1)



'''
DatabaseSqlalchemy Class:
-----------------------





'''




class DatabaseSqlalchemy():


    def __init__(self,
                 dbname,
                 username,
                 password,
                 hostname = 'localhost',
                 portnumber = 5433):


    ## Setup psycopg2 database connection.

        self.conn = self.connect_to_postgres_through_sqlalchemy(
                                                              username,
                                                              password,
                                                              dbname,
                                                              hostname,
                                                              portnumber)

        self.rconn = self.conn.raw_connection()

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
        con = create_engine(url, client_encoding='utf8')
        con.connect()
        # We then bind the connection to MetaData()
        meta = MetaData(bind=con, reflect=True)
        #print(meta)
        return(con)


    def get_table_list_as_dataframe(self, schemaname):

        vals = {'schemaname' : schemaname}

        query = "SELECT tablename FROM pg_catalog.pg_tables where schemaname =:schemaname"
        res = pd.read_sql_query(sa.text(query), self.conn, params = vals)
        return(res)

    def get_column_list_for_table_as_dataframe(self, tablename):

        vals = {'tablename': tablename}

        query = "SELECT column_name FROM information_schema.columns WHERE " \
                "table_name=:tablename"
        res = pd.read_sql_query(sa.text(query), self.conn, params=vals)
        return (res)


    def _build_table_class(self, tablename, fields):
        Base = declarative_base()

        class NewTable(Base):
            __tablename__ = tablename

            id = Column(Integer, primary_key=True)

        for index, row in fields.iterrows():
            setattr(NewTable, row['fields'], Column(String(255)))


        Base.metadata.create_all(self.conn)










'''
DatabaseAsyncpg Class:
-----------------------





'''


class DatabaseAsyncpg():


    def __init__(self,
                 dbname,
                 username,
                 password,
                 hostname = 'localhost',
                 portnumber = 5433):


    ## Setup psycopg2 database connection.

        self.conn = self.connect_to_postgres_through_psycopg2(dbname,
                                                              username,
                                                              password,
                                                              hostname,
                                                              portnumber)


    def connect_to_postgres_through_psycopg2(self,
                                             dbname,
                                             username,
                                             password,
                                             hostname='localhost',
                                             portnumber=5433):
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

        return (conn)
