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
from google.cloud import bigquery
from google.cloud import storage 
from pathlib import Path 

def moveFile(bucket_from, bucket_to, files_path):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_from)
    target_bucket = storage_client.bucket(bucket_to)
    #print('source_bucket: ', source_bucket)
    blobs = source_bucket.list_blobs(prefix = files_path, delimiter = '/') 

    for blob in blobs:
        #print('blob is: ', blob.name)
        #print('files_path: ', files_path)
        if (blob.name != files_path): 
            source_bucket.copy_blob(blob, target_bucket, blob.name)
            source_bucket.delete_blob(blob.name)
            print('Moved blob ', blob.name, ' to bucket ', target_bucket, ', Path ', files_path)

print('Number of arguments: ', len(sys.argv), 'arguments.')
print('Argument List: ', str(sys.argv))

# TODO: Error if argument count mis-match
if(len(sys.argv) == 5):
    triggerFileLocation = sys.argv[1]
    bqProject = sys.argv[2]
    historicalLoadConfigFileUri = sys.argv[3]
    datasetName = sys.argv[4]
else:
    raise Exception("Number of arguments should be 5")

print('Trigger file name: ', triggerFileLocation)
print('BQ ProjectName: ', bqProject)
print('HistoricalLoad ConfigFile: ', historicalLoadConfigFileUri)
print('historicalLoad Dataset: ', datasetName)

# Construct a BigQuery client object.
client = bigquery.Client(project=bqProject)
print("Client created using project: {}".format(client.project))

##################################################
# Read the config file and load it into a BQ table
##################################################

# Generate a unique name for the configFileTable
jobUuid = str(uuid.uuid4().int)
configFileTableNamePart = 'HistoricalLoad_config_'+jobUuid
configFileTable = datasetName+'.'+configFileTableNamePart
print('Config file table name: ',configFileTable)

# Create the load job config and load the table into BQ
configFile = open(historicalLoadConfigFileUri, 'rb')
loadconfig = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True)
config_job = client.load_table_from_file(configFile, configFileTable,
                    job_config=loadconfig)  
print('Starting job {}'.format(config_job.job_id))    
config_job.result()                
configFile.close()
#######################################################
# Query the config table for the current run properties
#######################################################
totalNewRowsAdded = 0
my_file = Path(triggerFileLocation)
if my_file.is_file():
    triggerFile = open(triggerFileLocation, 'r')
    Lines = triggerFile.readlines()
    print(Lines)
    for line in Lines:
        tableInfoStr = line.strip().split(';')
        print('tableStr: ',tableInfoStr)
        tableToLoad = tableInfoStr[0]
        partitionFolder = tableInfoStr[1]
        print('partitionFolder: ',partitionFolder)
        query = 'SELECT * FROM '+configFileTable+' where JobID = @job_id and Active = @activeFlag ';
        #print('Query is ',query);

        query_config = bigquery.QueryJobConfig(query_parameters=[
                            bigquery.ScalarQueryParameter('job_id', 'STRING', tableToLoad),
                            bigquery.ScalarQueryParameter('activeFlag', 'BOOL', 'true')
                        ], use_legacy_sql=False)
        queryResult = client.query(query, job_config=query_config)
        for row in queryResult:
            #print('Config is: ',row)
            dataSet = row.DataSet
            dataFilePath = row.DataFilePath
            insertQuery = row.InsertQuery
            sourceFormat = row.SourceFormat
            sourceBucket = row.SourceBucket
            processedBucket = row.ProcessedBucket
            errorBucket = row.ErrorBucket
            archiveFiles = row.ArchiveFiles
            active = row.Active

        # Fetch the location of the data files (excluding the source bucket name)
        # Eg: dataFilePath = 'gs://<bucket-name>/Input/ncvdc62_fnv2_ev_usa_sec_hte_ext_bad/'
        #     relativeFilesPath should be set to Input/ncvdc62_fnv2_ev_usa_sec_hte_ext_bad/
        completePath = dataFilePath+partitionFolder+'/'
        pathArray = completePath.replace('gs://', '').split('/')
        relativeFilesPath = '/'.join(pathArray[1:])
        print('relativeFilesPath: ', relativeFilesPath)
        getDataset = client.dataset(dataSet)
        tableBeforeQuery = client.get_table(getDataset.table(tableToLoad))
        print('Total number of rows in table before executing the query: ', tableBeforeQuery.num_rows)

        # Configure the external data source and query job.
        try:
            uri = dataFilePath+partitionFolder+'/*'
            print('URI is: ',uri)
            external_config = bigquery.ExternalConfig(sourceFormat)
            external_config.source_uris = [
                uri
            ]
            table_id = "TestOrcExt"
            job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

            # Replace the source table name and the target table name
            insertQuery = insertQuery.replace('EXTERNAL_TABLE', table_id)
            insertQuery = insertQuery.replace('TARGET_TABLE', dataSet+'.'+tableToLoad)
            #print('Insert Query is: ',insertQuery)

            query_job = client.query(insertQuery, job_config=job_config)  # Make an API request.

            w_states = list(query_job)  # Wait for the job to complete.
            print('Success: Data inserted into ', tableToLoad)
            
            tableAfterQuery = client.get_table(getDataset.table(tableToLoad))
            newRowsAdded = tableAfterQuery.num_rows - tableBeforeQuery.num_rows
            print('Total number of rows in table after executing the query: ', tableAfterQuery.num_rows)
            print('Rows added for partition:', newRowsAdded)
            totalNewRowsAdded = totalNewRowsAdded +newRowsAdded
            
            
            if (archiveFiles):
                print('Moving the files to processed bucket: ', processedBucket, ', Path: ', relativeFilesPath)
                moveFile(sourceBucket, processedBucket, relativeFilesPath)
        except Exception as e:
            print('Error: ', e)
            if (archiveFiles):
                print('Moving the files to error bucket: ', errorBucket, ', Path: ', relativeFilesPath)
                moveFile(sourceBucket, errorBucket, relativeFilesPath)
    triggerFile.close()
else:
    raise Exception(triggerFileLocation + ' ' + 'not found')

print('Total number of new rows added in table:', totalNewRowsAdded)

### Delete the temp config table
client.delete_table(configFileTable, not_found_ok=True)





