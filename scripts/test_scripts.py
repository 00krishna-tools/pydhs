
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller
from pydhs.controller_countrydata import Controller_countrydata

def main_add_country_data(database):

    print('completed addition of country data.')

if __name__ == "__main__":
    main_add_country_data('db_dhs_global')
