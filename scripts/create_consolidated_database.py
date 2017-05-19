
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
from pydhs.Controller import Controller

def main(database, tablefile):




    c = Controller(database)

    #c.action_build_union_fields_table('union_table', tablefile)

    c.action_build_intersection_fields_table('intersection_table', tablefile)

    c.action_get_variables_by_table_csv_file()

    #c.action_insert_data_to_table(tablefile, 'union_table')

    c.action_insert_data_to_table(tablefile, 'intersection_table')
    c.action_add_wealth_id_column_to_intersection_table()
    c.action_add_wealth_wlthindf_column_to_intersection_table()
    c.action_add_wealth_wlthind5_column_to_intersection_table()


if __name__ == "__main__":
    # execute only if run as a script
    main('db_dhs_global', 'tablelist.csv')
