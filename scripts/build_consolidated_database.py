
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller


def main(database, tablefile):


    c = Controller(database)

















if __name__ == "__main__":
    # execute only if run as a script
    main('db_antonio_india', 'tablelist.csv')
