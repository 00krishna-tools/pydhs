
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller
from pydhs.controller_countrydata import Controller_countrydata

def main_add_country_data(database):
    d = Controller_countrydata(database)
    d.action_update_iso3_codes_for_country_data()
    d.get_country_column_names_and_add_to_intersection_table()
    d.create_query_for_merging_country_data_into_intersection_table()
    d.merge_country_data_into_intersection_table()
    print('completed addition of country data.')

if __name__ == "__main__":
    main_add_country_data('db_dhs_global')
