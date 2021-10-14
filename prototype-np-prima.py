import pygsheets
import datetime

run_interfal = 120 # in minutes

last_run = datetime.datetime.now() - datetime.timedelta(minutes=run_interfal)

gc = pygsheets.authorize(service_file='credentials.json')

sh = gc.open("Prototype PN-PRIMA")
wks = sh.sheet1

data = wks.get_all_values(
    include_tailing_empty_rows=False, include_tailing_empty=False, returnas='matrix'
)[1:]

update_data = []

for i in range(len(data) -1, -1, -1):
    if(datetime.datetime.strptime(data[i][0], "%m/%d/%Y %X") < last_run):
        break
    update_data.append(data[i])

print(update_data)