from google.cloud import bigquery
from google.cloud.exceptions import NotFound

client = bigquery.Client()
def main(): 
    dataset_id = "experiment-328903.test_dataset"
    table_id = "experiment-328903.test_dataset.test_table"

    print("Checking if dataset {} exist".format(dataset_id))
    if(not check_dataset_exist(dataset_id)):
        print("Dataset {} Not Exist, Creating new Dataset !!!".format(dataset_id))
        create_dataset()
        
    else:
        print("Dataset {} Exist!!".format(dataset_id))

    if(not check_table_exist(table_id)):
        create_table()
    else:
        print("Table {} Exist!!".format(table_id))

    # query_job = client.query(
    #     """
    #     INSERT {}.{} VALUES('test','test','tes')""".format(dataset,table)
    # )

    # results = query_job.result()

    # for row in results:
    #     print("{} : {} views".format(row.url, row.view_count))

def create_dataset(dataset_id):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'asia-southeast2'
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def create_table(table_id):
    table = bigquery.Table(table_id)

    schema = [
        bigquery.SchemaField("full_name","STRING", mode="REQUIRED"),
        bigquery.SchemaField("age","INTEGER", mode="REQUIRED")
      ]
      
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def check_dataset_exist(dataset_id):
    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
        return True
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))
        return False

def check_table_exist(table_id):
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False

main()