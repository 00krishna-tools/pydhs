
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller
from pydhs.controller_countrydata import Controller_countrydata

def main_build_country_data(database):
    d = Controller_countrydata(database)
    d.action_add_column_iso3_to_intersection_table()
    d.update_country_data_ihme_table_year_field()
    d.create_table_country_data_from_joining_data_sources()

    print('country data table built.')

if __name__ == "__main__":
    main_build_country_data('db_dhs_global')
