
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller

def main_merge(database):
    c = Controller(database)
    c.action_merge_wealth_data_into_birth_table()
    print('merge of wealth and birth data completed.')

if __name__ == "__main__":
    main_merge('db_dhs_global')
