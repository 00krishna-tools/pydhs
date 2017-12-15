

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
    print('wealth intersection table built.')

    d =

if __name__ == "__main__":
    main_wealth('db_dhs_global', 'tablelists/tablelist_wi.csv')
