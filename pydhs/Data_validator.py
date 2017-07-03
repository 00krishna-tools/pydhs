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



from pydhs.Controller import Controller

class Data_Validator():

    def __init__(self, dbname, var):

        self.c = Controller(dbname)

        self.var = var


    def plot_dist_of_target_variable(self, var):

