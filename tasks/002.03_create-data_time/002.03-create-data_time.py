import os
import sys

DIR_TASK = os.path.basename(os.getcwd())
DIR_LIB = os.path.abspath(os.path.join(os.path.dirname(__file__),"../"))
DIR_TASK = os.path.dirname(os.path.abspath(__file__))

import json, csv, time, string, itertools, copy, yaml
import pyodbc
import numpy as np
import pandas as pd
import datetime as dt

config = yaml.load( stream = file( DIR_TASK + '\\config.yml', 'r'))
#yaml.dump( config, file( DIR_TASK + '\\config.yml', 'w') )

sys.path.append( DIR_LIB )

from lib.router import Router
router = Router( )

# --------------------------------------------------------------------------

#STEP: modify version?

configVersion = config['version']
config['version'] =  round( float(configVersion) + .1, 1 ) if config['options']['increment-version'] == True else configVersion


#STEP: Save dta to DB

cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=qlik_medical;Trusted_Connection=yes;')
#cnxn.setencoding(encoding='utf-8')
cursor = cnxn.cursor()

#com: Create temporary table

targetTable = 'D_TIME_TEMP'

if cursor.tables(table=targetTable, tableType='TABLE').fetchone() is None:
  queryStr = """
  CREATE TABLE {_table}
  (
    ID_TIME INT NOT NULL IDENTITY(1,1),
    MONTH INT NOT NULL,
    YEAR INT NOT NULL,
    CONSTRAINT D_TIME_TEMP_PK PRIMARY KEY (ID_TIME)
  )
  """
  cursor.execute( queryStr.format( _table= targetTable)  )
  cnxn.commit()
else:
  queryStr = "TRUNCATE TABLE {_table}"
  cursor.execute( queryStr.format( _table= targetTable) )
  cnxn.commit()


#com: Build insert query
listYears = [2016,2017,2018]
listMonths = range(1,13)

listRowValues = []

for y in range(2016,2019):
  for m in range(1,13):
    listRowValues.append( [m,y] )

queryStr = """
INSERT INTO [dbo].[{_table}]
  ( [MONTH], [YEAR] )
VALUES
  ( ?, ? )
"""
cursor.executemany( queryStr.format( _table= targetTable), listRowValues )
cnxn.commit()

#com: close db connection
cnxn.close()


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config.yml', 'w'), indent=2, default_flow_style=False )