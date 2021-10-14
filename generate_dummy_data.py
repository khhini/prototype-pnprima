import pandas as pd, numpy as np
import random, requests
from bs4 import BeautifulSoup
from faker import Faker
import datetime
import pygsheets


### Generate Data Dummy
fake = Faker()

nama_kader = []
nama_pasien = []
id_pasien = []
jenis_penyakit = ["Diabetes", "Hipertensi", "TB"]
kondisi = ["Berat", "Sedang", "Ringan"]
Kode_puskesmas = ['JKT', 'BTM', 'BDG']
jenis_kelamin = ["Laki - Laki", "Perempuan"]

n_names = 10

for i in range(10):
    nama_kader.append(fake.name())
    nama_pasien.append(fake.name())
    id_pasien.append(Kode_puskesmas[i%3] + "-" + str(random.randint(100, 999)))


claim_jenis_penyakit = np.random.choice(jenis_penyakit, n_names)
claim_kondisi = np.random.choice(kondisi, n_names)
claim_jenis_kelamin = np.random.choice(jenis_kelamin, n_names)

dummy = []
for i in range(n_names):
    dummy.append([datetime.datetime.now().strftime("%m/%d/%Y %X"), nama_kader[i], id_pasien[i], nama_pasien[i], claim_jenis_penyakit[i], claim_kondisi[i], random.randint(10,60), claim_jenis_kelamin[i]])


### Add dummy data to google Sheets

gc = pygsheets.authorize(service_file='credentials.json')

sh = gc.open("Prototype PN-PRIMA")
wks = sh.sheet1

for d in dummy:
    wks.append_table(d)

print(wks.get_all_values(
    include_tailing_empty_rows=False, include_tailing_empty=False, returnas='matrix'
))
