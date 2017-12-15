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


class Controller_stored_procedures():
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


    def add_four_digit_function(self):

        query = """
            
            create or replace function 4_digit_date(dt TEXT)
                returns TEXT
                as 
                $$
                DECLARE
                    intDate INT;
                    newDate TEXT;
                BEGIN     
                
                
                
                    intDate = dt::INT
                
                    IF intDate > 1000 THEN
                        RETURN dt
                
                    IF intDate < 18 THEN
                        intDate = intDate + 2000
                    ELSIF intDate > 18 THEN      
                        intDate = intDate + 1900
                
                    RETURN intDate::TEXT
                
                END;
                $$ 
                LANGUAGE plpgsql;  
                """

        self.db.get_regular_cursor_query_no_return(query)
