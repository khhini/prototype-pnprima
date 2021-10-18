from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def create_dataset(dataset_id):
    print("Creating dataset {}".format(dataset_id))

    client = bigquery.Client()
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'asia-southeast2'
    dataset = client.create_dataset(dataset, timeout=30)

    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def create_table(table_id):
    print("Creating table {}".format(table_id))

    client = bigquery.Client()
    table = bigquery.Table(table_id)

    schema = [
        bigquery.SchemaField("nama_kader","STRING", mode="REQUIRED"),
        bigquery.SchemaField("id","STRING", mode="REQUIRED"),
        bigquery.SchemaField("nama_pasien","STRING", mode="REQUIRED"),
        bigquery.SchemaField("jenis_penyakit","STRING", mode="REQUIRED"),
        bigquery.SchemaField("kondisi","STRING", mode="REQUIRED"),
        bigquery.SchemaField("usia_pasien","INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("jenis_kelamin","STRING", mode="REQUIRED"),
      ]
      
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def check_dataset_exist(dataset_id):
    client = bigquery.Client()
    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
        return True
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))
        return False

def check_table_exist(table_id):
    client = bigquery.Client()
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False

def insert_to_table(dataset_id, table_id, data_to_insert):
    client = bigquery.Client()
    print("Preparing...")
    print("====================================================")

    print("Checking if dataset {} exist".format(dataset_id))
    if(not check_dataset_exist(dataset_id)):
        print("Dataset {} Not Exist, Creating new Dataset !!!".format(dataset_id))
        create_dataset(dataset_id)

    print("Checking if dataset {} exist".format(dataset_id))
    if(not check_table_exist(table_id)):
        create_table(table_id)

    print("====================================================\n")
    print("Insert {} rows to Table {}".format(len(data_to_insert), table_id))
    errors = client.insert_rows_json(table_id, data_to_insert)
    
    print("\n")

    if errors == []:
        print("{} New rows have been added.".format(len(data_to_insert)))
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def delete_table(table_id):
    client = bigquery.Client()

    client.delete_table(table_id, not_found_ok=True)
    print("Delete table '{}'",format(table_id))

# if __name__ == "__main__":
#     table_id = "experiment-328903.np_prima_dataset.np_prima_table"
#     delete_table(table_id)