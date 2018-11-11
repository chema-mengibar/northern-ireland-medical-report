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

#STEP: get LCG values from DB

cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=qlik_medical;Trusted_Connection=yes;')
cursor = cnxn.cursor()

cursor.execute("SELECT * FROM A_LCG")

queryResult = cursor.fetchall()
lcgDict = {}
for item in queryResult:
  lcgDict[ item[1] ] = item[0]


#STEP: modify version?

configVersion = config['version']
config['version'] =  round( float(configVersion) + .1, 1 ) if config['options']['increment-version'] == True else configVersion


#STEP: Collect all source-files as a single dataframe

dfs = []

def sourceToDf( _year, _month ):
  #params: files_2018, april
  inputFilePath =  router.getRoute( config['source']['route'] ) \
    + config['source']['dir'] \
    + config['source'][_year][_month]
  return pd.read_csv( filepath_or_buffer=inputFilePath, sep=',', quoting=0 ) #,encoding="utf-8-sig"

yearFileKey = 'files_2018'
for iFileKey in config['source'][ yearFileKey ]:
  dfBuffer = sourceToDf( yearFileKey, iFileKey )
  dfs.append( dfBuffer )

yearFileKey = 'files_january'
for iFileKey in config['source'][ yearFileKey ]:
  dfBuffer = sourceToDf( yearFileKey, iFileKey )
  dfs.append( dfBuffer )

panDf = pd.concat( dfs )

#com: Remove rows if colum LGC value is Nan
dfAll = panDf.dropna(subset=['Postcode', 'LCG'])
dfB =  dfAll.drop_duplicates(subset='Postcode', keep="last")

#com: create new table for db import
dfPostcodes = dfB[['LCG', 'Postcode']].copy()
dfPostcodes['ID_LCG'] = dfPostcodes['LCG'].apply(lambda x: lcgDict[x])
dfPostcodes['DATE_FROM'] = dt.datetime.now().strftime("%Y-%m-%d")
dfPostcodes['DATE_TO'] = dt.datetime.now().strftime("%Y-%m-%d")
dfPostcodes['VERSION'] = 1
dfPostcodes = dfPostcodes.rename(columns={'Postcode': 'POSTCODE'})

dfClean = dfPostcodes.drop(['LCG'], axis=1)


#STEP: output-file

outputPath =  router.getRoute( config['target']['route'] ) \
+ config['target']['dir'] \

outputFilePath = outputPath \
+ config['target']['file'].replace("$VERSION$", str( config['version'] ) )

#com: create output folder
if not os.path.exists( outputPath ):
  os.makedirs( outputPath )

#com: save file
dfClean.to_csv( path_or_buf=outputFilePath, sep=",", quoting=None, index=False )


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config.yml', 'w'), indent=2, default_flow_style=False )