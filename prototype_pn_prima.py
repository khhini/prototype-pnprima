import pygsheets
import datetime
from bq_operation import insert_to_table
import yaml


if __name__ == "__main__":

    gc = pygsheets.authorize(service_file='credentials.json')

    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    dataset_id = config["bq_config"]["dataset_id"]
    table_id = config["bq_config"]["table_id"]
    log_file = config["log"]["log_file"]
    table_field = [x["field_name"] for x in config["bq_config"]["table_field"]]

    sh = gc.open(config["gsheet_config"]["file_name"])
    wks = sh.sheet1

    raw_data = wks.get_all_values(
        include_tailing_empty_rows=False, include_tailing_empty=True, returnas='matrix'
    )[1:]

    if(len(raw_data) > 0):

        with open(log_file, "r") as f:
            logs = f.readlines()
            last_inserted_timestamp = datetime.datetime.strptime(
                logs[-1].split(",")[-1][25:].strip(), "%m/%d/%Y %X")

        last_data_timestamp = raw_data[-1][0]

        data_to_insert = []

        for i in range(len(raw_data) - 1, -1, -1):
            if(datetime.datetime.strptime(raw_data[i][0], "%m/%d/%Y %X") <= last_inserted_timestamp):
                break
            row = {}
            for x in range( len(table_field)):
                if (x == 0):
                    row[table_field[x]] = datetime.datetime.timestamp(datetime.datetime.strptime(raw_data[i][x], "%m/%d/%Y %X"))
                else:
                    row[table_field[x]] = raw_data[i][x] if raw_data[i][x] != "" else None
            data_to_insert.append(row)

        print("Found {} rows to Insert".format(len(data_to_insert)))
        data_to_insert = list(reversed(data_to_insert))

        if(len(data_to_insert) > 0):
            res = insert_to_table(dataset_id, table_id, data_to_insert)

            with open("pn_prima_job.log", "a") as f:
                f.write("{}, {}: {}, LAST_INSERTED_TIMESTAMP:{}\n".format(
                    datetime.datetime.now(), res[0], res[1], last_data_timestamp if res[0] == "INFO" else last_inserted_timestamp.strftime("%m/%d/%Y %X")))
            print(res[0], res[1])
        else:
            with open("pn_prima_job.log", "a") as f:
                f.write("{}, {}: {}, LAST_INSERTED_TIMESTAMP:{}\n".format(
                    datetime.datetime.now(), "INFO", "No Data to Insert", last_inserted_timestamp.strftime("%m/%d/%Y %X")))
            print("No Data to Insert")

    else:
        with open("pn_prima_job.log", "a") as f:
            f.write("{}, {}: {}, LAST_INSERTED_TIMESTAMP:{}\n".format(datetime.datetime.now(
            ), "INFO", "No Data to Insert", datetime.datetime.now().strftime("%m/%d/%Y %X")))
        print("Found 0 rows to Insert")
        print("No Data to Insert")
