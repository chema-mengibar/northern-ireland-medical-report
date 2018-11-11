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

config = yaml.load( stream = file( DIR_TASK + '\\config_create.yml', 'r'))
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


#STEP: Collect all source-files as a single dataframe

def sourceToDf( _file, _folderYear, _yearFileKey ):
  #params: files_2018, april              
  inputFilePath =  router.getRoute( config['source']['route'] ) \
    + config['source']['dir'] \
    + str(_folderYear) + '\\' \
    + _file
  dfBuf0 = pd.read_csv( filepath_or_buffer=inputFilePath, sep=',',  quoting=0) 
  dfBuf1 = dfBuf0[[ 'BNF Chapter', 'BNF Section', 'BNF Paragraph' ]].copy()
  del [[dfBuf0 ]]
  gc.collect()
  dfBuf1.columns = [ 'CHAPTER', 'SECTION', 'PARAGRAPH' ]
  return dfBuf1

#com: start load
listDfs201x = []

yearFileKey = config['params']['targetGroupFiles'] #WARN: used in the output-file name
year = int( yearFileKey[-4:] )

for iFile in config['source'][ yearFileKey ]:
  dfBuffer = sourceToDf( iFile, year, yearFileKey )
  listDfs201x.append( dfBuffer )
df1x = pd.concat( listDfs201x )

#com: 
print len( df1x )

#com: Clean rows and columns
dfZ = df1x
del [[ dfBuffer, listDfs201x, df1x ]]
gc.collect()


dfZV1 = dfZ.dropna( axis=0, subset=['CHAPTER', 'SECTION', 'PARAGRAPH' ], how='all')
dfZV2 =  dfZV1.groupby(['CHAPTER', 'SECTION', 'PARAGRAPH' ]).size().to_frame('size').reset_index()

dfZV3 = dfZV2[ ( dfZV2[['CHAPTER', 'SECTION', 'PARAGRAPH']] != '-').all(axis=1) ]
dfZV3[['CHAPTER', 'SECTION', 'PARAGRAPH']] = dfZV3[['CHAPTER', 'SECTION', 'PARAGRAPH']].apply(pd.to_numeric, downcast='integer')
dfZV4 = dfZV3.drop(['size'], axis=1)

print dfZV4.head(5)
print len( dfZV4 )

#com: free memory
del [[ dfZ, dfZV1, dfZV2, dfZV3 ]]
gc.collect()


#STEP: output-file

outputPath =  router.getRoute( config['target']['route'] ) \
+ config['target']['dir'] \

outputFilePath = outputPath \
+ config['target']['file'].replace("$NAME$", yearFileKey)

#com: create output folder
if not os.path.exists( outputPath ):
  os.makedirs( outputPath )

#com: save file
dfZV4.to_csv( path_or_buf=outputFilePath, sep="|", quoting=None, index=False )


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config_create.yml', 'w'), indent=2, default_flow_style=False )