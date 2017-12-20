"""
Data Validator Class
-----------------

This class will validate that the distributions in the original tables match the distributions of the merged data.
"""

__author__ = 'krishnab'
__version__ = '0.1.0'

import numpy as np
import pandas as pd
import os
import datetime
import psycopg2 as pg
#import pydhs.DbTable as DTable
import psycopg2
from psycopg2 import sql
from pydhs.Database import DatabasePsycopg2
from pydhs.Database import DatabaseSqlalchemy
import sqlalchemy
from functools import reduce
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.extensions import AsIs
from bokeh.io import output_file, show
from bokeh.layouts import gridplot
from bokeh.palettes import Viridis3
from bokeh.plotting import figure
from ggplot import aes, diamonds, geom_density, ggplot
import matplotlib.pyplot as plt
from bokeh import mpl
from pydhs.Controller import Controller

class Data_Validator():

    def __init__(self, dbname, tablelist, varlist):

        self.c = Controller(dbname)

        self.varlist = list(pd.read_csv(varlist).values.flatten())
        self.tables = list(pd.read_csv(tablelist).values.flatten())

    def plot_dist_of_target_variable(self, var):

        if var not in self.varlist:
            print('please reference a variable that is in the intersection table.')
            return

        output_file("variable_distributions.html")



    def build_list_of_plots(self, var):

        plots = []

        for tbl in self.tables:
            df_from_original_table_on_variable = pd.DataFrame(self.get_variable_data_from_original_table(tbl, var))
            df_from_intersection_on_variable = pd.DataFrame(self.get_variable_data_from_intersection_table(var))

            df_from_intersection_on_variable.append(df_from_intersection_on_variable)
            g = ggplot(df_from_intersection_on_variable, aes(x='var', color='table')) + geom_density()
            g.make()


    def get_variable_data_from_original_table(self, tablename, var):

        query = """SELECT %s FROM %s;"""

        return(self.c.db.get_regular_cursor_query(query, (AsIs(var), AsIs(tablename),)))

    def get_variable_data_from_intersection_table(self, var):

        query = """SELECT %s FROM intersection_table_birth;"""

        return (self.c.db.get_regular_cursor_query(query, (AsIs(var),)))



if __name__ == "__main__":
    # execute only if run as a script
    main('db_dhs_global', 'tablelists/tablelist_br.csv', )
