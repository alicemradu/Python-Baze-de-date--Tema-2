from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Conectare la MongoDB
client = MongoClient("mongodb://master:stud1234@193.226.34.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
db = client['daune_leasing']
collection = db['clienti_daune']

# Definirea pipeline-ului pentru agregare
pipeline = [
    {
        "$group": {
            "_id": {
                "marca": "$MARCA",
                "model": "$MODEL"
            },
            "valoare_totala": {"$sum": "$VALOARE_DAUNA"},
            "numar_daune": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "marca": "$_id.marca",
            "model": "$_id.model",
            "valoare_totala": 1,
            "numar_daune": 1
        }
    }
]

# Executarea agregarii
cursor = collection.aggregate(pipeline)

# Convertirea rezultatelor intr-un DataFrame Pandas
df = pd.DataFrame(list(cursor))

# Asigurarea că coloana 'model' este tratată ca sir de caractere
df['model'] = df['model'].astype(str)

# Filtrarea autoturismelor pentru care valoarea totala a daunelor depaseste 50.000$
autoturisme_valoare_mare = df[df['valoare_totala'] > 50000]
numar_autoturisme_valoare_mare = autoturisme_valoare_mare.shape[0]

# Afișarea numarului de autoturisme
print(f"Numarul de autoturisme pentru care valoarea totala a daunelor depaseste 50.000$: {numar_autoturisme_valoare_mare}")

# Afișarea DataFrame-ului cu autoturismele care au valoarea totala a daunelor mai mare de 50.000$
print("Autoturismele cu valoarea totala a daunelor mai mare de 50.000$: \n", autoturisme_valoare_mare)

# Filtrarea si reprezentarea grafica a modelelor care au inregistrat mai mult de 150 de daune
modele_daune_multe = df[df['numar_daune'] > 150]

# Afișarea DataFrame-ului cu modelele care au inregistrat mai mult de 150 de daune
print("Modelele care au inregistrat mai mult de 150 de daune: \n", modele_daune_multe)

# Reprezentarea grafica
plt.figure(figsize=(12, 8))
plt.barh(modele_daune_multe['model'], modele_daune_multe['numar_daune'], color='skyblue')
plt.xlabel('Număr de daune')
plt.ylabel('Model')
plt.title('Modelele care au inregistrat mai mult de 150 de daune')
plt.tight_layout()
plt.show()

# Salvare DataFrame in fisier CSV pentru analiza ulterioara
#df.to_csv('rezultate_daune.csv', index=False)
