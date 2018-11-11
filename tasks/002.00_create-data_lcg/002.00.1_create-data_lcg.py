import os
import sys

DIR_TASK = os.path.basename(os.getcwd())
DIR_LIB = os.path.abspath(os.path.join(os.path.dirname(__file__),"../"))
DIR_TASK = os.path.dirname(os.path.abspath(__file__))

import json, csv, time, string, itertools, copy, yaml
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
dfAll = panDf.dropna(subset=['LCG'])

#com: Create the data for the SQL-table
listLcg =  dfAll['LCG'].unique()
listIndex = range(1, len( listLcg )+1 )

dfData = {
#  'ID_LCG': listIndex,
  'LCG': listLcg,
  'VERSION': 1,
  'DATE_FROM': dt.datetime.now().strftime("%Y-%m-%d") ,
  'DATE_TO': dt.datetime.now().strftime("%Y-%m-%d") ,
}
dfNew = pd.DataFrame( dfData, columns=['LCG', 'VERSION', 'DATE_FROM', 'DATE_TO' ] ) 
dfNewSort = dfNew.sort_values(by=['LCG'])


#STEP: output-file

outputPath =  router.getRoute( config['target']['route'] ) \
+ config['target']['dir'] \

outputFilePath = outputPath \
+ config['target']['file'].replace("$VERSION$", str( config['version'] ) )

#com: create output folder
if not os.path.exists( outputPath ):
  os.makedirs( outputPath )

#com: save file
dfNewSort.to_csv( path_or_buf=outputFilePath, sep=",", quoting=None, index=False )


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config.yml', 'w'), indent=2, default_flow_style=False )