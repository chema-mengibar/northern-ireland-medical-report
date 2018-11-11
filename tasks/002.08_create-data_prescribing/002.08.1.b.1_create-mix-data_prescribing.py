import os
import sys

DIR_TASK = os.path.basename(os.getcwd())
DIR_LIB = os.path.abspath(os.path.join(os.path.dirname(__file__),"../"))
DIR_TASK = os.path.dirname(os.path.abspath(__file__)) + '\\'

import gc
import json, csv, time, string, itertools, copy, yaml, re
import pyodbc
import numpy as np
import pandas as pd
import datetime as dt
from time import clock
import threading

CONFIG_FILE = 'config_1.b.1.yml'
config = yaml.load( stream = file( DIR_TASK + CONFIG_FILE, 'r'))
#yaml.dump( config, file( DIR_TASK + CONFIG_FILE, 'w') )

sys.path.append( DIR_LIB )

from lib.router import Router
router = Router( )

# ---------------------------------- INIT
print '>> Walk, Don`t Run'


#----------------------------------- STEP: modify version?

configVersion = config['version']
config['version'] =  round( float(configVersion) + .1, 1 ) if config['options']['increment-version'] == True else configVersion

pItemsFileLimit = config['params']['itemsFileLimit']

#----------------------------------- STEP: Collect dimension tables

dfTables = {}
for iFile in config['source']['tables'][ 'files' ]:
  inputFilePath =  router.getRoute( config['source']['tables']['route'] ) \
    + config['source']['tables']['dir'] \
    + iFile
  tableName = iFile.replace('table--','')
  tableName = tableName.replace('.csv','')
  
  dfTables[ tableName ] = pd.read_csv( filepath_or_buffer=inputFilePath, sep=';',  quoting=0) 

#help: dfTables KEYS -> D_AMP_NM, D_BNF, D_COST, D_PRACTICE, D_TIME, D_VMP_NM, D_VTM_NM
# print dfTables['D_PRACTICE']


#----------------------------------- STEP: Collect all source-files as a single dataframe

#com: start load
#warning: files are too big to load more than one, actually should be just 1 item in this loop

def sourceToDf(  _folderYear, _file ):
  #params: files_2018, april              
  inputFilePath =  router.getRoute( config['source']['prescripciones']['route'] ) \
    + config['source']['prescripciones']['dir'] \
    + str(_folderYear) + '\\' \
    + _file

  dfBuffer =  pd.read_csv( filepath_or_buffer=inputFilePath, sep=',',  quoting=0 ) 

  #com: rename PRACTICE -> Practice
  #https://stackoverflow.com/questions/21285380/pandas-find-column-whose-name-contains-a-specific-string
  #https://stackoverflow.com/questions/20868394/changing-a-specific-column-name-in-pandas-dataframe/34192820
  r = re.compile("Practice|PRACTICE")
  columnName = list(filter(r.match, dfBuffer.columns ))
  return dfBuffer.rename(columns = { columnName[0]: 'Practice'})




#----------------------------------- STEP: join tables-data with main-dataframe to replace the IDs

#help: df:index->0 means file:row->2 ( header-row ) 
#help: dfTables KEYS -> D_AMP_NM, D_BNF, D_COST, D_PRACTICE, D_TIME, D_VMP_NM, D_VTM_NM
def getItemData( _dfRowItem, itemIdx ):

  #option1: param:_itemCursor  and item-reference:  dfPcb.iloc[ _itemCursor ]
  #option2: item-row to paramseter

  # task: ID_PRACTICE
  itemPractice = _dfRowItem['Practice'] #Dirty
  ID_PRACTICE = dfTables['D_PRACTICE'].loc[ (dfTables['D_PRACTICE']['ID_PRACTICE_ORG'] == itemPractice ) ]['ID_PRACTICE'].values[0]

  # task: ID_AMP_NM
  itemAmp = _dfRowItem['AMP_NM']
  
  if itemAmp != '-':
    #com: clean item value
    itemAmp = itemAmp.strip()
    temp_ID_AMP_NM = dfTables['D_AMP_NM'].loc[ (dfTables['D_AMP_NM']['AMP_NM'] == itemAmp ) ]['ID_AMP_NM']
    ID_AMP_NM = temp_ID_AMP_NM.values[0]
  else:
    ID_AMP_NM = ''


  # task: ID_TIME
  itemYear = _dfRowItem['Year']
  itemMonth = _dfRowItem['Month']
  if itemYear != '-' and itemMonth != '-':
    ID_TIME = dfTables['D_TIME'].loc[ (dfTables['D_TIME']['MONTH'] == itemMonth ) & (dfTables['D_TIME']['YEAR'] == itemYear ) ]['ID_TIME'].values[0]
  else:
    ID_TIME = ''


  # task: ID_VMP_NM
  itemVmp = _dfRowItem['VMP_NM']
  if itemVmp != '-':
    #com: clean item value
    itemVmp = itemVmp.strip()
    ID_VMP_NM = dfTables['D_VMP_NM'].loc[ (dfTables['D_VMP_NM']['VMP_NM'] == itemVmp ) ]['ID_VMP_NM'].values[0]
  else:
    ID_VMP_NM = ''


  # task: ID_VTM_NM
  itemVtm = _dfRowItem['VTM_NM']
  if itemVtm != '-':
    #com: clean item value
    itemVtm = itemVtm.strip()
    ID_VTM_NM = dfTables['D_VTM_NM'].loc[ (dfTables['D_VTM_NM']['VTM_NM'] == itemVtm ) ]['ID_VTM_NM'].values[0]
  else:
    ID_VTM_NM = ''


  # task: ID_INTERVAL_COST
  costColumn = dfPcb.filter(regex=("Gross.*")).columns[0]
  itemInterval = _dfRowItem[ costColumn ]
  if itemInterval != '-':
    #com: clean item value
    ID_INTERVAL_COST = dfTables['D_COST'].loc[ (dfTables['D_COST']['INTERVAL_COST'] == itemInterval ) ]['ID_INTERVAL_COST'].values[0]
  else:
    ID_INTERVAL_COST = ''


  # task: ID_BNF
  itemChapter = _dfRowItem['BNF Chapter']
  itemSection = _dfRowItem['BNF Section']
  itemParagraph = _dfRowItem['BNF Paragraph']

  if itemChapter != '-' and itemSection != '-' and itemParagraph != '-' :
    #com: clean item value
    try:
      ID_BNF__TEMP = dfTables['D_BNF'].loc[ 
        (dfTables['D_BNF']['SECTION'] == itemSection ) 
        & (dfTables['D_BNF']['CHAPTER'] == itemChapter ) 
        & (dfTables['D_BNF']['PARAGRAPH'] == itemParagraph ) ]['ID_BNF']
      ID_BNF = ID_BNF__TEMP.values[0]
    except:
      print '>>>>>>>>>>> exception on BNF'
      ID_BNF = ''    
  else:
    ID_BNF = ''

  #com:  Rest columns
  TOTAL_ITEMS = _dfRowItem[ 8 ]
  TOTAL_QUANTITY = _dfRowItem[ 9 ]
  GROSS_COST = _dfRowItem[ 10 ]
  ACTUAL_COST = _dfRowItem[ 11 ]

  return [ 
    ID_AMP_NM, 
    ID_VMP_NM, 
    ID_VTM_NM, 
    ID_INTERVAL_COST, 
    ID_BNF, 
    ID_PRACTICE, 
    ID_TIME, 
    TOTAL_ITEMS, 
    TOTAL_QUANTITY, 
    GROSS_COST, 
    ACTUAL_COST  
  ]

def counter():
  tim = threading.Timer(5.0, counter)
  tim.start()
  #os.system('cls' if os.name == 'nt' else 'clear')
  print '>> Loaded items:' + str( _rowIndex ) + ' of ' + str( pItemsFileLimit )
  if _rowIndex > pItemsFileLimit:
    tim.cancel()

#com: save data in intervals
def saveOutputfile( _header=False ):
  #com: build all rows into dataframe
  dfZ1 = pd.DataFrame( dataRows, columns=colNames )

  outputPath =  router.getRoute( config['target']['route'] ) \
  + config['target']['dir'] \

  fileSufix = pYearFileKey
  outputFilePath = outputPath + config['target']['file'].replace("$NAME$", fileSufix )

  #com: create output folder
  if not os.path.exists( outputPath ):
    os.makedirs( outputPath )

  #com: delete contents for first time, then add data 
  writeFlag = 'w' if _header else 'a'  

  #com: save file
  with open( outputFilePath, writeFlag ) as streamFile:
    dfZ1.to_csv( path_or_buf=streamFile, sep=";", quoting=None, index=False, header=_header, decimal=',' )
  
  del [[ dfZ1 ]]
  del dataRows[:]
  gc.collect()

colNames = [ 
  'ID_AMP_NM', 
  'ID_VMP_NM', 
  'ID_VTM_NM', 
  'ID_INTERVAL_COST', 
  'ID_BNF', 
  'ID_PRACTICE', 
  'ID_TIME', 
  'TOTAL_ITEMS', 
  'TOTAL_QUANTITY', 
  'GROSS_COST', 
  'ACTUAL_COST'  
]




#----------------------------------- STEP: output-file

#com: read precripcions file
pYearFileKey = config['params']['targetGroupFiles'] #WARN: used in the output-file name
pYearFileIndex = config['params']['indexes'][pYearFileKey]

year = int( pYearFileKey[-4:] )
selectedFile = config['source']['prescripciones'][ pYearFileKey ][pYearFileIndex]
dfPcb = sourceToDf( year, selectedFile )

#STEP: execute joins
#help: itemCursor = 393684
#help: print getItemData( itemCursor )

#com: join fields in row-items
dataRows = []

#com: terminal progress counter
totalRows = len( dfPcb )
divisions = 1000
segmentRows = totalRows / divisions
currentSegmentRows = segmentRows
segment = 1
_rowIndex = 0

print 'Total rows ' + str( totalRows )

#com: init counter
counter()

#com: initial file save to write headers and empty data
if pYearFileIndex == 0:
  saveOutputfile( _header=True )

#com: script clock start
_cb1 = clock()

#com: loop data
'''com: segments represent groups of the total rows. After each segment, will be the content 
added into the output-file'''

for rowIndex, rowItem in dfPcb.iterrows():
  dataRows.append( getItemData( rowItem, rowIndex ) )
  _rowIndex = rowIndex
  if rowIndex > currentSegmentRows:
    segment += 1
    currentSegmentRows = segment * segmentRows
    saveOutputfile(  )
  if rowIndex > pItemsFileLimit:
    #com: script clock stop
    _cb2 = clock()
    print 'Script time: ' + str( dt.timedelta( seconds=(_cb2 - _cb1) ) )
    config['params']['indexes'][pYearFileKey] = pYearFileIndex + 1
    break  
 

#STEP: update config file

yaml.dump( config, file( DIR_TASK + CONFIG_FILE, 'w'), indent=2, default_flow_style=False )