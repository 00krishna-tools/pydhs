
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller

def main_add_country_data(database):
    c = Controller(database)
    c.action_add_columns_for_country_data()
    c.action_update_iso3_codes_for_country_data()
    c.action_clean_year_values_in_intersection_table()
    c.action_merge_country_data_into_birth_table()
    print('completed addition of country data.')

if __name__ == "__main__":
    main_add_country_data('db_dhs_global')
