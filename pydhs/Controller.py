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
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.extensions import AsIs



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
                                    5432)


        self.conn_sqlalchemy = DatabaseSqlalchemy(dbname,
                                                  'krishnab',
                                                  '3kl4vx71',
                                                  'localhost',
                                                  5432)

        self.database_table_fields = {}

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

        self.database_table_fields = table_fields

        #print(table_fields)
        return(table_fields)


    def action_get_intersection_of_fields_across_database_tables(self,
                                                                 tablefile):

        tables = self.action_get_variable_names_for_each_table_in_database(
            tablefile)

        fields = [value for key, value in tables.items()]

        intersected_columns = pd.DataFrame(list(
            self.get_intersection_of_setlist(fields)))
        intersected_columns.columns = ['fields']

        return(intersected_columns)


    def action_get_union_of_fields_across_database_tables(self,tablefile):

        tables = self.action_get_variable_names_for_each_table_in_database(
            tablefile)

        fields = [value for key, value in tables.items()]

        union_columns = pd.DataFrame(list(self.get_union_of_setlist(fields)))
        union_columns.columns = ['fields']
        print(union_columns['fields'])
        return(union_columns)


    def action_build_union_fields_table(self, tablename,tablefile):

        fields = self.action_get_union_of_fields_across_database_tables(
            tablefile).sort_values('fields', ascending=True)

        # Note that there is a hard limit in postgres on 1600 columns in a table

        fields.to_csv('unionVariableList.csv', header=True)

        self.db.check_existence_or_drop_query(tablename)

        self.conn_sqlalchemy._build_table_class(tablename, fields[:1599])

    def action_build_intersection_fields_table(self, tablename, tablefile):

        fields = self.action_get_intersection_of_fields_across_database_tables(
            tablefile).sort_values('fields', ascending=True)

        fields.to_csv('intersectionVariableList.csv', header=True)

        self.db.check_existence_or_drop_query(tablename)

        self.conn_sqlalchemy._build_table_class(tablename, fields)


    def action_insert_data_to_table(self, tablefile, destination_table):

        ## First, update the table and field names in case of changes.

        self.action_get_variable_names_for_each_table_in_database(tablefile)

        ##

        t = set(self.db.get_table_column_names(
            destination_table))

        destination_table_fields = set([v[0] for v in t])
        query_list = []

        for key, value in self.database_table_fields.items():

            s = [v[0] for v in value]
            setlist = [set(destination_table_fields), set(s)]

            fields = self.get_intersection_of_setlist(setlist)

            fields = list(fields)

            string_fields = ','.join(map(str, fields))

            query = "insert into " + str(destination_table) + " (" + string_fields + \
                    ") " + \
                    "select " + string_fields + " from " + key + ";"

            query_list.append(query)

        #print(query_list)

        for q in query_list:
            self.db.get_regular_cursor_query_no_return(q)


    def action_add_table_name_to_each_database_table(self, tablefile):

        tables = pd.read_csv(tablefile)

        for tbl in tables['tablename']:

            query = """DO
            $$
            BEGIN
            IF not EXISTS (SELECT column_name
                           FROM information_schema.columns
                           WHERE table_schema='public' and table_name=%s and
                           column_name='tablename') THEN
            alter table %s add column tablename varchar default null ;
            else
            raise NOTICE 'Already exists';
            END IF;
            END
            $$"""

            self.db.get_regular_cursor_query_no_return(query, (tbl, AsIs(tbl)))

        for tbl in tables['tablename']:
            query2 = "UPDATE %s SET tablename = %s;"
            self.db.get_regular_cursor_query_no_return(query2, (AsIs(tbl), str(tbl)))

        return(0)

    def action_get_variables_by_table_csv_file(self):

        self.db.get_variables_by_table().to_csv('variablesByTable.csv')

    def action_set_table_names_to_lowercase(self):

        self.db.set_all_table_names_to_lowercase()


    def action_set_field_names_to_lowercase(self):

        self.db.set_all_field_names_to_lowercase()


    def action_standardize_fields(self):

        table_fields_list = []
        for idx, tbl in enumerate(TABLENAMES):

            table_fields_list.append(self.db.get_table_column_names(
                tbl))


        for idx, tbl in enumerate(TABLENAMES):

            query_list = []

            if ('b4',) in table_fields_list[idx]:
                query_list.append("""update %s set b4 = lower(b4)""")

            if ('v102',) in table_fields_list[idx]:
                query_list.append("""update %s set v102 = lower(
                v102)""")

            if ('v190',) in table_fields_list[idx]:
                query_list.append("""update %s set v190 = 'Lowest
                quintile' where v190 = '1'""")

                query_list.append("""update %s set v190 = 'Fourth
                quintile' where v190 = '2'""")

                query_list.append("""update %s set v190 = 'Middle
                quintile' where v190 = '3'""")

                query_list.append("""update %s set v190 = 'Second
                quintile' where v190 = '4'""")

                query_list.append("""update %s set v190 = 'Highest
                quintile' where v190 = '5'""")

                query_list.append("""update %s set v190 = 'Lowest
                    quintile' where v190 = 'poorest'""")

                query_list.append("""update %s set v190 = 'Fourth
                    quintile' where v190 = 'poorer'""")

                query_list.append("""update %s set v190 = 'Middle
                    quintile' where v190 = 'middle'""")

                query_list.append("""update %s set v190 = 'Second
                    quintile' where v190 = 'richer'""")

                query_list.append("""update %s set v190 = 'Highest
                    quintile' where v190 = 'richest'""")

            if ('v106',) in table_fields_list[idx]:

                query_list.append("""update %s set v106 = 'no education' where
                v106 = '0'""")

                query_list.append("""update %s set v106 = 'primary' where v106 =
                '1'""")

                query_list.append("""update %s set v106 = 'secondary' where v106 =
                '2'""")

                query_list.append("""update %s set v106 = 'higher' where v106 =
                '3'""")

                query_list.append("""update %s set v106 = NULL where v106 = '9'""")

            for q in query_list:

                self.db.get_regular_cursor_query_no_return(q, (AsIs(tbl), ))
                print(q)

            del(query_list)

    def action_add_wealth_id_column_to_intersection_table(self):
        query = """DO $$ 
                BEGIN
                    BEGIN
                        ALTER TABLE intersection_table ADD COLUMN whhid text;
                    EXCEPTION
                        WHEN duplicate_column THEN RAISE NOTICE 'column wwhid already exists in intersection_table.';
                    END;
                END;
            $$"""

        self.db.get_regular_cursor_query_no_return(query)

    def action_add_wealth_wlthindf_column_to_intersection_table(self):
        query = """DO $$ 
                BEGIN
                    BEGIN
                        ALTER TABLE intersection_table ADD COLUMN wlthindf text;
                    EXCEPTION
                        WHEN duplicate_column THEN RAISE NOTICE 'column wlthindf already exists in intersection_table.';
                    END;
                END;
            $$"""

        self.db.get_regular_cursor_query_no_return(query)

    def action_add_wealth_wlthind5_column_to_intersection_table(self):
        query = """DO $$ 
                BEGIN
                    BEGIN
                        ALTER TABLE intersection_table ADD COLUMN wlthind5 text;
                    EXCEPTION
                        WHEN duplicate_column THEN RAISE NOTICE 'column wlthind5 already exists in intersection_table.';
                    END;
                END;
            $$"""

        self.db.get_regular_cursor_query_no_return(query)

    def action_add_wealth_v002_column_to_intersection_table(self):
        query = """DO $$ 
                BEGIN
                    BEGIN
                        ALTER TABLE intersection_table ADD COLUMN wv002 text;
                    EXCEPTION
                        WHEN duplicate_column THEN RAISE NOTICE 'column wv002 already exists in intersection_table.';
                    END;
                END;
            $$"""

        self.db.get_regular_cursor_query_no_return(query)

    def action_add_wealth_v003_column_to_intersection_table(self):
        query = """DO $$ 
                BEGIN
                    BEGIN
                        ALTER TABLE intersection_table ADD COLUMN wv003 text;
                    EXCEPTION
                        WHEN duplicate_column THEN RAISE NOTICE 'column wv003 already exists in intersection_table.';
                    END;
                END;
            $$"""

        self.db.get_regular_cursor_query_no_return(query)

    def action_separate_whhid_to_cluster_and_household_id(self):

        query = """UPDATE intersection_table 
       SET wv002 = (regexp_split_to_array(BTRIM(whhid), '\s+'))[1], 
       wv003 = (regexp_split_to_array(BTRIM(whhid), '\s+'))[2];"""

        self.db.get_regular_cursor_query_no_return(query)

    def action_rename_intersection_table(self, tablename):

        query = "ALTER TABLE IF EXISTS intersection_table RENAME TO %s;"

        self.db.get_regular_cursor_query_no_return(query, (AsIs(tablename)))


    def get_intersection_of_setlist(self,setlist):

        return(set.intersection(*setlist))


    def get_union_of_setlist(self, setlist):

        return (set.union(*setlist))







