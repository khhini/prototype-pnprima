from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import time
import yaml

key_path="credentials.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)


def create_dataset(dataset_id):
    print("Creating dataset {}".format(dataset_id))

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'asia-southeast2'
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def create_table(table_id, table_field):
    print("Creating table {}".format(table_id))

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table = bigquery.Table(table_id)

    schema = []
    
    for x in (table_field):
        schema.append(bigquery.SchemaField(x["field_name"], x["type"], mode=x["mode"]))

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, timeout=30)
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def check_dataset_exist(dataset_id):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
        return True
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))
        return False

def check_table_exist(table_id):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False

def insert_to_table(dataset_id, table_id, data_to_insert):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

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
        return ["INFO", "{} New rows have been added.".format(len(data_to_insert))]
    else:
        return ["ERROR", "Encountered errors while inserting rows: {}".format(errors)]

def delete_table(table_id):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    client.delete_table(table_id, not_found_ok=True)
    print("Delete table '{}'",format(table_id))

# if __name__ == "__main__":
#     with open("config.yaml", "r") as f:
#         config = yaml.load(f, Loader=yaml.FullLoader)
#     dataset_id = config["bq_config"]["dataset_id"]
#     table_id = config["bq_config"]["table_id"]
#     table_field = config["bq_config"]["table_field"]
#     delete_table(table_id)
#     create_table(table_id, table_field)
    
    