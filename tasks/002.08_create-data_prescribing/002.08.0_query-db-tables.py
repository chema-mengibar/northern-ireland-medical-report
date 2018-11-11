import os
import sys

DIR_TASK = os.path.basename(os.getcwd())
DIR_LIB = os.path.abspath(os.path.join(os.path.dirname(__file__),"../"))
DIR_TASK = os.path.dirname(os.path.abspath(__file__))

import gc
import json, csv, time, string, itertools, copy, yaml
import pyodbc
import numpy as np
import pandas as pd
import datetime as dt
from time import clock

config = yaml.load( stream = file( DIR_TASK + '\\config_queries.yml', 'r'))

sys.path.append( DIR_LIB )

from lib.router import Router
router = Router( )

# --------------------------------------------------------------------------
print '>> Walk, Don`t Run'

# _cb2 = clock()
# print( ( _cb2 - _cb1 ) )

#STEP: modify version?

configVersion = config['version']
config['version'] =  round( float(configVersion) + .1, 1 ) if config['options']['increment-version'] == True else configVersion


#STEP: output-file directory

#com: set output path
outputPath =  router.getRoute( config['target']['route'] ) \
+ config['target']['dir']

#com: create output folder
if not os.path.exists( outputPath ):
  os.makedirs( outputPath )


#STEP: read table

#com: connect to db
cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=qlik_medical;Trusted_Connection=yes;')
cursor = cnxn.cursor()

def tableToDf( _tableName ):
  #com: build query
  queryStr = "SELECT * FROM {_table}"
  cursor.execute( queryStr.format( _table= _tableName) )
  queryResult = cursor.fetchall()
  #com: build dataframe
  queryData =   [ list(x) for x in queryResult] 
  queryDescriptor =  cursor.description 
  columNames =  list( map(lambda x: x[0], queryDescriptor) ) 
  return pd.DataFrame( queryData, columns= columNames )

def tableToCsv( _tableName ):
  #com: read rows and create a dataframe
  dfBuffer = tableToDf( _tableName )

  #com: save df to csv
  outputFilePath = outputPath + config['target']['file'].replace("$NAME$", _tableName )

  #com: save file
  dfBuffer.to_csv( path_or_buf=outputFilePath, sep=";", quoting=None, index=False )

  #com: free memory
  del [[ dfBuffer ]]
  gc.collect()


for table in config['params']['tables']:
  print( '>> table: ', table )
  tableToCsv( table )


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config_queries.yml', 'w'), indent=2, default_flow_style=False )