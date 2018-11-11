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

config = yaml.load( stream = file( DIR_TASK + '\\config_join.yml', 'r'))
#yaml.dump( config, file( DIR_TASK + '\\config.yml', 'w') )

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

#com: set target column name
COLUMN_NAME = config['params']['column']

#STEP: Collect all source-files as a single dataframe

def sourceToDf( _file ):
  inputFilePath =  router.getRoute( config['source']['route'] ) \
    + config['source']['dir'] \
    + _file
  return pd.read_csv( filepath_or_buffer=inputFilePath, sep='|',  quoting=0 ) 
  

#com: start load
listDfs = []

for iFile in config['source'][ 'files' ]:
  dfBuffer = sourceToDf( iFile )
  listDfs.append( dfBuffer )
dfA1 = pd.concat( listDfs )

#com: 
print len( dfA1 )

#com: Clean rows and columns
dfZ = dfA1

del [[ dfBuffer, listDfs, dfA1 ]]
gc.collect()

dfZV2 =  pd.DataFrame( list(dfZ[COLUMN_NAME].unique()), columns=[COLUMN_NAME] ) 
dfZV3 =  dfZV2.sort_values([COLUMN_NAME], ascending=True)

#com: free memory
del [[ dfZ, dfZV2 ]]
gc.collect()

#com: 
print len( dfZV3 )
dfZV3['DATE_FROM'] = dt.datetime.now().strftime("%Y-%m-%d")
dfZV3['DATE_TO'] = dt.datetime.now().strftime("%Y-%m-%d")
dfZV3['VERSION'] = 1

#STEP: output-file

outputPath =  router.getRoute( config['target']['route'] ) \
+ config['target']['dir'] \

outputFilePath = outputPath \
+ config['target']['file'].replace("$VERSION$", str( config['version'] ) )

#com: create output folder
if not os.path.exists( outputPath ):
  os.makedirs( outputPath )

#com: save file
dfZV3.to_csv( path_or_buf=outputFilePath, sep="|", quoting=None, index=False )


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config_join.yml', 'w'), indent=2, default_flow_style=False )



