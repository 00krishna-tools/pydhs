
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
    c.action_get_variables_by_table_csv_file()
    c.action_insert_data_to_table(tablefile, 'intersection_table')
    c.action_add_wealth_id_column_to_intersection_table()
    c.action_add_wealth_wlthindf_column_to_intersection_table()
    c.action_add_wealth_wlthind5_column_to_intersection_table()
    c.action_rename_intersection_table('intersection_table_birth')


if __name__ == "__main__":
    # execute only if run as a script
    main_wealth('db_dhs_global', 'tablelist_wi.csv')
    main_birth('db_dhs_global', 'tablelist_br.csv')
