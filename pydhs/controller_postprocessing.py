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


class Controller_postprocessing():
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

    def update_b2_dates_to_four_digits(self):

        query = """
            update
                intersection_table_birth
            set 
                b2 = four_digit_date(trim(b2));
        """
        self.db.get_regular_cursor_query_no_return(query)

    def update_v007_dates_to_four_digits(self):

        query = """
            update
                intersection_table_birth
            set 
                v007 = four_digit_date(trim(v007));
        """
        self.db.get_regular_cursor_query_no_return(query)
