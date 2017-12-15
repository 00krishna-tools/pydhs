from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller
from pydhs.controller_postprocessing import Controller_postprocessing

def main_birth(database, tablefile):

    c = Controller(database)

    c.action_build_intersection_fields_table('intersection_table', tablefile)
    c.action_insert_data_to_table(tablefile, 'intersection_table')
    c.action_add_wealth_id_column_to_intersection_table()
    c.action_add_wealth_wlthindf_column_to_intersection_table()
    c.action_add_wealth_wlthind5_column_to_intersection_table()
    c.action_rename_intersection_table('intersection_table_birth')
    c.action_add_columns_for_merge()

    d = Controller_postprocessing(database)
    d.update_b2_dates_to_four_digits()
    d.update_v007_dates_to_four_digits()
    d.update_v190_recode()

    print("birth intersection table built.")

if __name__ == "__main__":
    # execute only if run as a script
    main_birth('db_dhs_global', 'tablelists/tablelist_br.csv')
