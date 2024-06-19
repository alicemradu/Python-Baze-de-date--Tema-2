import pandas as pd
from pymongo import MongoClient

# Conectarea la MongoDB
client = MongoClient(
    "mongodb://master:stud1234@193.226.34.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
db = client['daune_leasing']
collection = db['clienti_daune']

try:
    # Interogarea datelor din colecția clienti_daune pentru marcile TOYOTA, FORD și HONDA
    cursor = collection.find(
        {"MARCA": {"$in": ["TOYOTA", "FORD", "HONDA"]}},
        {"MARCA": 1, "MODEL": 1, "AN_FABRICATIE": 1, "COMPONENTA": 1, "PRET_TOTAL": 1, "PRET_MANOPERA": 1, "_id": 0}
    )

    # Convertirea la DataFrame Pandas
    df = pd.DataFrame(list(cursor))

    # Setarea opțiuni afisare randuri si coloane
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', None)

    # Verificarea dacă DataFrame-ul nu este gol
    if not df.empty:
        # Calcularea procentului manoperei din prețul total
        df['PROCENT_MANOPERA'] = (df['PRET_MANOPERA'] / df['PRET_TOTAL']) * 100

        # Afișarea DataFrame-ului rezultat
        print(df)

        # Verificarea numarului de intrari pentru fiecare marca
        print("Numar de intrari pe marcă:")
        print(df['MARCA'].value_counts())
    else:
        print("Nu s-au gasit date pentru marcile specificate.")
except Exception as e:
    print(f"A aparut o eroare: {e}")
