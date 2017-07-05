
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller

def main_wealth(database, tablefile):

    c = Controller(database)

    c.action_build_intersection_fields_table('intersection_table', tablefile)
    c.action_get_variables_by_table_csv_file()
    c.action_insert_data_to_table(tablefile, 'intersection_table')
    c.action_add_wealth_id_column_to_intersection_table()
    c.action_add_wealth_v002_column_to_intersection_table()
    c.action_add_wealth_v003_column_to_intersection_table()
    c.action_separate_whhid_to_cluster_and_household_id()
    c.action_rename_intersection_table('intersection_table_wealth')

def main_birth(database, tablefile):

    c = Controller(database)

    c.action_build_intersection_fields_table('intersection_table', tablefile)
    c.action_insert_data_to_table(tablefile, 'intersection_table')
    c.action_add_wealth_id_column_to_intersection_table()
    c.action_add_wealth_wlthindf_column_to_intersection_table()
    c.action_add_wealth_wlthind5_column_to_intersection_table()
    c.action_rename_intersection_table('intersection_table_birth')
    c.action_add_columns_for_merge()
    print("all done.")

def main_merge(database):
    c = Controller(database)
    c.action_merge_wealth_data_into_birth_table()

def main_add_country_data(database):
    c = Controller(database)
    #c.action_add_columns_for_country_data()
    #c.action_update_iso3_codes_for_country_data()
    c.action_clean_year_values_in_intersection_table()

if __name__ == "__main__":
    # execute only if run as a script
#    main_wealth('db_dhs_global', 'tablelists/tablelist_wi.csv')
#    main_birth('db_dhs_global', 'tablelists/tablelist_br.csv')
#    main_merge('db_dhs_global')
    main_add_country_data('db_dhs_global')
