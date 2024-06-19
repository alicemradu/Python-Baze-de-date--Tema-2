from pymongo import MongoClient
import pandas as pd

# Conectarea la MongoDB
client = MongoClient("mongodb://master:stud1234@193.226.34.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
db = client['daune_leasing']
collection = db['clienti_leasing']

# Definirea interogarii
query = {
    'VARSTA': {'$gt': 45},
    'SUMA_DEPOZIT': {'$gt': 35000}
}

# Definirea proiectiei
projection = {
    '_id': 0,
    'NUME_CLIENT': 1,
    'SUMA_SOLICITATA': 1,
    'SUMA_DEPOZIT': 1,
    'PRESCORING': 1
}

# Executarea interogarii
results = collection.find(query, projection)

# Incarcarea datelor intr-un DataFrame Pandas
df = pd.DataFrame(list(results))
pd.set_option('display.max_rows', None)

# Verificarea structurii DataFrame-ului
print("Structura initiala a DataFrame-ului:")
print(df.head())

# Verificarea si modificarea presoringului
df.loc[df['SUMA_DEPOZIT'] > df['SUMA_SOLICITATA'], 'PRESCORING'] = 7
# Afisarea DataFrame-ului modificat
print("Structura DataFrame-ului dupa modificare:")
print(df)
