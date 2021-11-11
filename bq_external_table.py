###########################################################################################
# Arguments:                                                                              
#   trigger file name
###########################################################################################
import sys
import uuid
from google.cloud import bigquery

print('Number of arguments:', len(sys.argv), 'arguments.')
print('Argument List:', str(sys.argv))
print('Argument 1:', str(sys.argv[1]))  # 1st argument

# Construct a BigQuery client object.
client = bigquery.Client(project="ford-gcp-ingest-d")
print("Client creating using default project: {}".format(client.project))

# Construct the source URI from the 1st argument
# (Sample trigger file format: table=NCVD035_PJ1_VIN_LIST&cvdc62_partition_month_x=2021-04.trg)
# 1st Argument format: <bucketname>/<other-path>/tablename-partitionname
# 1. Fetch the tablename and partitionname
# 2. Get the configurations for the table from HistoricalLoad_config.json
# 3. Load the .json and get the value for the datafiles root path
# 4. Add the current partition name to the root path
# 5. Create temporary external table for the partition datafiles
# 6. Insert the data from interim to final using the query in config file

bqProject = 'ford-gcp-ingest-d'
datasetName = 'Test'
tableName = 'HistoricalLoad_config'
configFileUri = 'https://storage.cloud.google.com/mukta-historical-load/config/Historical_Load_config.json'
configFileTable = 'Test.HistoricalLoad_config'

################################################
# Read the config file and load it into a BQ table
################################################
# TODO: Get the jobname from the pubsub event name
loadToRun = 'ncvdc62_fnv2_ev_usa_sec_hte_ext'
jobUuid = str(uuid.uuid4().int)
configFileTable = 'Test.HistoricalLoad_config'+'_'+jobUuid
print('Config file table name: ',configFileTable)

#tableId = TableId.of(BQproject,datasetName, tableName);
loadconfig = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True)
config_job = client.load_table_from_uri(configFileUri, configFileTable,
                    job_config=loadconfig)  
print('Starting job {}'.format(config_job.job_id))    
config_job.result()                
# Retrieve the config table
configFileTableId = client.get_table(configFileTable)

################################################
# Query the config table for the current run properties
################################################
query = 'SELECT * FROM '+configFileTable+' where JobID = @job_id and Active = @activeFlag ';
print('Query is ',query);

query_config = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter('job_id', 'STRING', loadToRun),
                    bigquery.ScalarQueryParameter('activeFlag', 'BOOL', 'true')
                ], use_legacy_sql=False)
queryResult = client.query(query, job_config=query_config)
print('The query data: ')
for row in queryResult:
   print('Config is: ',row)
   print('Data file path: ',row.DataFilePath)
   print('Data file archive path: ',row.DataFileArchivePath)
   print('Insert query: ',row.InsertQuery)
#print('Insert Query is: ',insertQuery)

# Configure the external data source and query job.
uri = 'gs://mukta-poc-sqlserver/Input/ncvdc62_fnv2_ev_usa_sec_hte_ext/cvdc62_partition_month_x=2021-04/000008_0'
external_config = bigquery.ExternalConfig("ORC")
external_config.source_uris = [
    uri
]
table_id = "TestOrcExt"
job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

# Example query to find states starting with 'W'.
#sql = 'SELECT * FROM `{}` WHERE name LIKE "W%"'.format(table_id)
sql = 'insert into Test.tmp_final (cvdc62_sha_k) select cvdc62_sha_k FROM `{}` '.format(table_id)

query_job = client.query(sql, job_config=job_config)  # Make an API request.

w_states = list(query_job)  # Wait for the job to complete.
print("There are {} rows inserted into final".format(len(w_states)))