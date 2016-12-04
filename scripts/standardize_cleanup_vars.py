
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller

def main(database):


    c = Controller(database)

    c.action_standardize_fields()



if __name__ == "__main__":
    # execute only if run as a script
    main('db_antonio_india')
