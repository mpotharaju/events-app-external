###########################################################################################
# Arguments:                                                                              
#   1. trigger file name
#   2. bq_project_name
#   3. historical load config file
#   4. historical load dataset name
# Logic:
# (Sample trigger file format: 
#   table=ncvdc62_fnv2_ev_usa_sec_hte;cvdc62_partition_month_x=2021-04.trg)
# 1st Argument format: <bucketname>/<other-path>/tablename-partitionname
# Construct the source URI from the 1st argument
# 1. Fetch the tablename and partitionname
# 2. Get the configurations for the table from HistoricalLoad_config.json
# 3. Load the .json and get the value for the datafiles root path
# 4. Add the current partition name to the root path
# 5. Create temporary external table for the partition datafiles
# 6. Insert the data from interim to final using the query in config file
###########################################################################################
import sys
import uuid
import fnmatch
import json
import io
import os
from google.cloud import bigquery
from google.cloud import storage 
from pathlib import Path 

def moveFile(sourceUri, targetUri):
    storage_client = storage.Client()
    source_bucket_name = sourceUri.replace('gs://', '').split('/')
    print('source_bucket_name: ', source_bucket_name[0])
    target_bucket_name = targetUri.replace('gs://', '').split('/')
    source_bucket = storage_client.bucket(source_bucket_name[0])
    target_bucket = storage_client.bucket(target_bucket_name[0])
    print('source_bucket: ', source_bucket)
    print('target_bucket: ', target_bucket)

    pathArray = sourceUri.replace('*', '').replace('gs://', '').split('/')
    print('pathArray: ', pathArray)
    files_path = '/'.join(pathArray[1:])
    print('files_path: ', files_path)
    #blobs = source_bucket.list_blobs(prefix = files_path, delimiter = '/')
    blobs = source_bucket.list_blobs(prefix = files_path) 
    for blob in blobs:
        #print('blob is: ', blob.name)
        #print('files_path: ', files_path)
        if (blob.name != files_path): 
            source_bucket.copy_blob(blob, target_bucket, blob.name)
            source_bucket.delete_blob(blob.name)
            print('Moved blob ', blob.name, ' to bucket ', target_bucket, ', Path ', files_path)

print('Number of arguments: ', len(sys.argv), 'arguments.')
print('Argument List: ', str(sys.argv))

#todo: Error if argument count mis-match
if(len(sys.argv) == 2):
    jsonFile = sys.argv[1]
  #  bqProject = sys.argv[2]
   # historicalLoadConfigFileLocation = sys.argv[3]
   # datasetName = sys.argv[4]
##   raise Exception("Error: Number of arguments should be 5\nUsage: bq_historical_load.py <trigger-file-location> <project-name> <config-file-location> <datasetname>")
else:
    raise Exception("Error: Number of arguments should be 2\nUsage: bq_historical_load.py <json-file-location>")

print('Json file location: ', jsonFile)
# TOdo: throw error if file doesnt exist or not readable
my_file_config = Path(jsonFile)
if my_file_config.is_file():
    configFile = open(jsonFile)
    jsonData = json.load(configFile)
    sourceUris = jsonData['sourceUris']
    bqProject = jsonData['target']['projectId']
    datasetName = jsonData['target']['dataset']
    tableName = jsonData['target']['table']
    processedBucketUri = jsonData['processedBucket']
    print('Source URI: ', jsonData['sourceUris'])
    print('BQ ProjectID: ', jsonData['target']['projectId'])
    print('historicalLoad Dataset: ', datasetName)
    print('Table: ', tableName)
    print('processedBucketUri: ', processedBucketUri)
    configFile.close()
else:
   raise Exception(jsonFile + ' ' + 'not found')

# Construct a BigQuery client object.
client = bigquery.Client(project=bqProject)
print("Client created using project: {}".format(client.project))
getDataset = client.dataset(datasetName)
 
#Read insert query from file
configPath = Path(jsonFile).parent.resolve()
print('rootPath is: ', str(configPath))
mySqlFile = Path(os.path.join(configPath, tableName+'.sql'))
print('mySqlFile is: ', str(mySqlFile))
insertQuery = ''
if mySqlFile.is_file():
    sqlQueryFile = open(mySqlFile, 'r')
    insertQuery = sqlQueryFile.read()
    sqlQueryFile.close()
else:
    raise Exception(str(mySqlFile) + ' ' + 'not found')

totalNewRowsAdded = 0

for uri in sourceUris:
    print('sourceUri: ',uri)
    tableBeforeQuery = client.get_table(getDataset.table(tableName))
    #print('Total number of rows before query:', tableBeforeQuery.num_rows)
    try:
                print('URI is: ',uri)
                external_config = bigquery.ExternalConfig('ORC')
                external_config.source_uris = [
                    uri
                ]
                table_id = "TestOrcExt"
                job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

                # Replace the source table name and the target table name
                insertQuery = insertQuery.replace('EXTERNAL_TABLE', table_id)
                insertQuery = insertQuery.replace('TARGET_TABLE', datasetName+'.'+tableName)
                #print('Insert Query is: ',insertQuery)

                query_job = client.query(insertQuery, job_config=job_config)  # Make an API request.

                w_states = list(query_job)  # Wait for the job to complete.
                print('Success: Data inserted into ', tableName)
                
                tableAfterQuery = client.get_table(getDataset.table(tableName))
                #print('Total number of rows after query:', tableAfterQuery.num_rows)
                newRowsAdded = tableAfterQuery.num_rows - tableBeforeQuery.num_rows
                print('Source URI: ',uri +'\n'+ 'Number of rows added for the uri: ' ,newRowsAdded)
                totalNewRowsAdded = totalNewRowsAdded +newRowsAdded
                
                #if (archiveFiles):
                    # sourceUri=gs://<bucket>/Input/<Tablename>/*
                    # sourceFile=gs://<bucket>/Input/<Tablename>/<partition1>/file1, gs://<bucket>/Input/<Tablename>/<partition1>/file2,
                    #           gs://<bucket>/Input/<Tablename>/<partition2>/file1
                    # destination folder=gs://<processedBucket>/Input/<Tablename>/<partition1>/file1, gs://<processedBucket>/Input/<Tablename>/<partition1>/file2,
                    #           gs://<processedBucket>/Input/<Tablename>/<partition2>/file1
                #print('Moving the files to processed bucket: ', processedBucket, ', Path: ', relativeFilesPath)
                #archiveFiles = ''
                #if (archiveFiles):
                moveFile(uri, processedBucketUri)
    except Exception as e:
                print('Error: ', e)
                #if (archiveFiles):
                # print('Moving the files to error bucket: ', errorBucket, ', Path: ', relativeFilesPath)
                    #moveFile(sourceBucket, errorBucket, relativeFilesPath)
        #triggerFile.close()
    #else:
        #raise Exception(triggerFileLocation + ' ' + 'not found')

print('Total number of new rows added in table:', totalNewRowsAdded)



