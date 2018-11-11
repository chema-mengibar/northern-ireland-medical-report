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

cursor.execute("SELECT * FROM A_POSTCODE")

queryResult = cursor.fetchall()
#com: result as dicctionary
# queryDict = {}
# for item in queryResult:
#   queryDict[ item[1] ] = item[0]

#com: result as datafram
queryData =   [ list(x) for x in queryResult] 
queryDescriptor =  cursor.description 
columNames =  list( map(lambda x: x[0], queryDescriptor) ) 
queryDf = pd.DataFrame( queryData, columns= columNames )


#STEP: modify version?

configVersion = config['version']
config['version'] =  round( float(configVersion) + .1, 1 ) if config['options']['increment-version'] == True else configVersion


#STEP: Collect all source-files as a single dataframe

def sourceToDf( _year, _month ):
  #params: files_2018, april
  inputFilePath =  router.getRoute( config['source']['route'] ) \
    + config['source']['dir'] \
    + config['source'][_year][_month]
  return pd.read_csv( filepath_or_buffer=inputFilePath, sep=',', quoting=0 ) #,encoding="utf-8-sig"

dfs2018 = []
yearFileKey = 'files_2018'
for iFileKey in config['source'][ yearFileKey ]:
  dfBuffer = sourceToDf( yearFileKey, iFileKey )
  dfBuffer['month'] =  iFileKey
  dfs2018.append( dfBuffer )

dfBuf = pd.concat( dfs2018 )
#com: Clean columns
dfBuf['Registered_Patients'] = dfBuf['Registered_Patients'].fillna( dfBuf['Registered Patients']  )
dfBuf['PracticeName'] = dfBuf['PracticeName'].fillna( dfBuf['Practice Name']  )
dfBuf['PracticeName'] = dfBuf['PracticeName'].str.strip()
dfBuf = dfBuf.dropna(subset=['PracticeName'])
dfBuf['PracNo'] = dfBuf['PracNo'].fillna( dfBuf['Practice No']  )
dfBuf["PracNo"] = pd.to_numeric( dfBuf["PracNo"], downcast='integer')
dfBuf = dfBuf.drop(['Practice No','Registered Patients', 'Practice Name', 'Unnamed: 8', 'Unnamed: 9'], axis=1)
dfBuf["Registered_Patients"] = pd.to_numeric( dfBuf["Registered_Patients"], downcast='integer')
dfBuf['year'] = 2018


dfsOld = []
yearFileKey = 'files_old'
for iFileKey in config['source'][ yearFileKey ]:
  dfBuffer = sourceToDf( yearFileKey, iFileKey )
  dfBuffer['month'] =  iFileKey
  dfsOld.append( dfBuffer )

def sanitize(data):
  return str(data).replace(',','')

panBufO = pd.concat( dfsOld )
#com: Clean columns
panBufO = panBufO.rename(columns={ 'Registered Patients':'Registered_Patients' })
panBufO = panBufO.dropna(subset=['PracticeName'])
panBufO["PracNo"] = pd.to_numeric( panBufO["PracNo"], downcast='integer')
panBufO = panBufO.dropna(subset=['Registered_Patients'])
panBufO["Registered_Patients"] =  panBufO["Registered_Patients"].apply(sanitize)
panBufO["Registered_Patients"] = pd.to_numeric(panBufO['Registered_Patients'], downcast='integer')
panBufO['PracticeName'] = panBufO['PracticeName'].str.strip()
panBufO['year'] = 2017

#com: merge old and new entries
dfGlobal = pd.concat( [ dfBuf, panBufO ] )

#com: keep the last entries 
dfGlobal = dfGlobal.sort_values(['year','month'], ascending=False).drop_duplicates( subset='PracNo', keep="first" ).sort_index()
#com: remain the original id in an extra columnn
dfGlobal['ID_PRACTICE_ORG'] = dfGlobal.filter(regex=("Prac.*No")).copy()


#STEP: create new table for db import

dfPractice = dfGlobal[['PracticeName', 'Address1', 'Address2', 'Address3', 'Postcode','ID_PRACTICE_ORG']].copy()

def searchPostcode( itemPostcode ):
  return queryDf[ queryDf['POSTCODE'] == itemPostcode ]['ID_POSTCODE'].item()

dfPractice['Address1'] = dfPractice['Address1'].replace({'"':''}, regex=True)
dfPractice['Address2'] = dfPractice['Address2'].replace({'"':''}, regex=True)
dfPractice['Address3'] = dfPractice['Address3'].replace({'"':''}, regex=True)
dfPractice['ID_POSTCODE'] = dfPractice['Postcode'].apply( searchPostcode )
dfPractice['DATE_FROM'] = dt.datetime.now().strftime("%Y-%m-%d")
dfPractice['DATE_TO'] = dt.datetime.now().strftime("%Y-%m-%d")
dfPractice['VERSION'] = 1
dfPractice = dfPractice.rename(columns={'PracticeName': 'PRACTICENAME', 'Address1': 'ADDRESS1', 'Address2': 'ADDRESS2', 'Address3': 'ADDRESS3'}) 
dfClean = dfPractice.drop(['Postcode'], axis=1)

#STEP: output-file

outputPath =  router.getRoute( config['target']['route'] ) \
+ config['target']['dir'] \

outputFilePath = outputPath \
+ config['target']['file'].replace("$VERSION$", str( config['version'] ) )

#com: create output folder
if not os.path.exists( outputPath ):
  os.makedirs( outputPath )

#com: save file
dfClean.to_csv( path_or_buf=outputFilePath, sep="|", quoting=None, index=False )


#STEP: update config file

yaml.dump( config, file( DIR_TASK + '\\config.yml', 'w'), indent=2, default_flow_style=False )