import pygsheets
import datetime
from bq_operation import insert_to_table

if __name__ == "__main__":
    run_interfal = 60 # in minutes

    last_run = datetime.datetime.now() - datetime.timedelta(minutes=run_interfal)

    gc = pygsheets.authorize(service_file='credentials.json')

    sh = gc.open("Prototype PN-PRIMA")
    wks = sh.sheet1

    data = wks.get_all_values(
        include_tailing_empty_rows=False, include_tailing_empty=False, returnas='matrix'
    )[1:]

    data_to_insert = []

    for i in range(len(data) -1, -1, -1):
        if(datetime.datetime.strptime(data[i][0], "%m/%d/%Y %X") < last_run):
            break
        data_to_insert.append(
            {
                u"nama_kader": data[i][1], 
                u"id":data[i][2], 
                u"nama_pasien":data[i][3], 
                u"jenis_penyakit": data[i][4],
                u"kondisi": data[i][5],
                u"usia_pasien": data[i][6],
                u"jenis_kelamin": data[i][7],
            }
        )
    
    print("Found {} rows to Insert".format(len(data_to_insert)))

    dataset_id = "experiment-328903.np_prima_dataset"
    table_id = "experiment-328903.np_prima_dataset.np_prima_table"

    if( len(data_to_insert) > 0 ):
        insert_to_table(dataset_id, table_id, data_to_insert)
    else:
        print("No Data to Insert")
    