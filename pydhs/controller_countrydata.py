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


from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy


## Initialize Constants

TABLENAMES = ["union_table", "intersection_table"]


class Controller_countrydata():
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

        query = """CREATE TABLE IF NOT EXISTS country_data AS
                    SELECT *
                    FROM country_bj_regime
                    LEFT JOIN country_ihme ON country_bj_regime.countryisocode = country_ihme.iso3
                    AND country_bj_regime.year = country_ihme.year_ihme;"""

        self.db.get_regular_cursor_query_no_return(query)

        print('new table country_data created.')

    def get_country_column_names_and_add_to_intersection_table(self):

        columnames = self.db.get_table_column_names('country_data')

        for n in columnames:
            self.db.add_column_to_table('intersection_table_birth', n[0])

    def create_query_for_merging_country_data_into_intersection_table(self):

        columnames = self.db.get_table_column_names('country_data')

        query_columns = ''

        for n in columnames:
            column_entry_format = str(n[0]) + ' = country_data.' + str(n[0]) + ', '
            query_columns = query_columns + column_entry_format

        return(query_columns)

    def merge_country_data_into_intersection_table(self):

        query_columns = self.create_query_for_merging_country_data_into_intersection_table()
        query = 'UPDATE intersection_table_birth SET ' + query_columns[:-2] + ' ' +  'FROM country_data WHERE trim(intersection_table_birth.iso3) = trim(country_data.countryisocode) AND trim(b2) = trim(country_data.year);'

        #self.db.get_regular_cursor_query_no_return(query)

    def action_update_iso3_codes_for_country_data(self):
        query = """UPDATE 
	                intersection_table_birth
                    SET
	                    iso3 = trim(country_codes.iso3code)
                    FROM
	                    country_codes
                    WHERE
	                    substring(trim(v000) from 1 for 2) = trim(dhs_codes);"""

        self.db.get_regular_cursor_query_no_return(query)


    def action_add_column_iso3_to_intersection_table(self):

        query = """ALTER table 
	                    intersection_table_birth
                    ADD column iso3 text;"""

        self.db.get_regular_cursor_query_no_return(query)
