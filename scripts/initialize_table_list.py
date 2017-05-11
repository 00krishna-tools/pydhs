
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller


def main(database):

    print('This script will generate a file named tablelist.csv.\n This list '
          'contains all of the tables in the indicated database.\n If there '
          'are any tables in the list that you do not want to include in the '
          'consolidated dataset, \n please remove them from this list.' )

    c = Controller(database)

    try:
        c.action_write_table_list_to_csv()

    except:
        "An error occurred while generating the tablelist.csv file"


















if __name__ == "__main__":
    # execute only if run as a script
    main('db_dhs_global')
