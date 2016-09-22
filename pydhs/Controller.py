"""
Controller Class
-----------------

This class contains the controller logic for the application. This takes
input from the table and other interface objects and then manages change of
state for the database. It will also pass the state changes to the table
objects.

"""

__author__ = 'krishnab'
__version__ = '0.1.0'

import numpy as np
import pandas as pd
import os
import datetime
import psycopg2 as pg
import pydhs.Database as DBase
import pydhs.DbTable as DTable


## Initialize Constants




class Controller():
    def __init__(self, dbname):

        self.conn = DBase.Database.get_connection(dbname)
















