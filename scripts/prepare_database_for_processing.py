

from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller

def main(database, tablefile):

    c = Controller(database)

## function to add table names to each table.

    c.action_add_table_name_to_each_database_table(tablefile)

## Function to convert table names to lower case

    c.action_set_table_names_to_lowercase()

## Function to convert column names to lower case

    c.action_set_field_names_to_lowercase()








if __name__ == "__main__":
    # execute only if run as a script
    main('db_dhs_global', 'tablelists/tablelist.csv')
