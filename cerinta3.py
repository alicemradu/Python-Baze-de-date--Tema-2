from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Conectare la MongoDB
client = MongoClient("mongodb://master:stud1234@193.226.34.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
db = client['daune_leasing']
collection = db['clienti_leasing']

# Definirea pipeline-ului pentru agregare
pipeline = [
    {
        "$group": {
            "_id": "$PROFESIA",
            "stare_civila": {"$first": "$STARE_CIVILA"},
            "venit_anual": {"$sum": "$VENIT_ANUAL_RON"},
            "suma_depozite": {"$sum": "$SUMA_DEPOZIT"},
            "suma_solicitata": {"$sum": "$SUMA_SOLICITATA"}
        }
    },
    {
        "$addFields": {
            "profesia_clean": {
                "$function": {
                    "body": r"""
                        function(profesia) {
                            // Definim o functie pentru a elimina codul numeric
                            var match = /^([^\d]+)/.exec(profesia);
                            if (match) {
                                return match[1].trim();
                            }
                            return profesia.trim();
                        }
                    """,
                    "args": ["$_id"],
                    "lang": "js"
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,  # Exclude _id from final result
            "profesia": "$profesia_clean",
            "stare_civila": 1,
            "venit_anual": 1,
            "suma_depozite": 1,
            "suma_solicitata": 1
        }
    }
]

# Executarea agregării
cursor = collection.aggregate(pipeline)

# Convertirea rezultatelor într-un DataFrame Pandas
df = pd.DataFrame(list(cursor))

# Calculul gradului de îndatorare
df['grad_indatorare'] = (df['suma_solicitata'] / (df['venit_anual'] + df['suma_depozite'])) * 100

# Setarea opțiunilor pentru a afișa toate coloanele și rândurile
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Afișarea întregului DataFrame
print(df)

# Reprezentarea grafică a gradului de îndatorare pe fiecare categorie de stare civilă
plt.figure(figsize=(10, 6))
plt.bar(df['stare_civila'], df['grad_indatorare'], color='skyblue')
plt.xlabel('Stare Civilă')
plt.ylabel('Grad de îndatorare (%)')
plt.title('Grad de îndatorare pe fiecare categorie de stare civilă')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Opțional, puteți salva DataFrame-ul într-un fișier CSV pentru analiză ulterioară
df.to_csv('rezultate_agregare.csv', index=False)
