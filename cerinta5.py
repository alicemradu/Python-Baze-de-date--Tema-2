from pymongo import MongoClient
import pandas as pd
import plotly.express as px

# Conectare la MongoDB
client = MongoClient("mongodb://master:stud1234@193.226.34.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
db = client['daune_leasing']
collection = db['USA_TMY']

# Definirea pipeline-ului pentru agregare
pipeline = [
    {
        "$group": {
            "_id": {
                "Location": "$Location",
                "State": "$State"
            },
            "total_gas_heating": {"$sum": "$GasHeating"},
            "total_electric_heating": {"$sum": "$ElectricHeating"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "Location": "$_id.Location",
            "State": "$_id.State",
            "total_gas_heating": 1,
            "total_electric_heating": 1
        }
    }
]

# Executarea agregarii
cursor = collection.aggregate(pipeline)

# Convertirea rezultatelor intr-un DataFrame Pandas
df = pd.DataFrame(list(cursor))

# Afisarea DataFrame-ului
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)
print(df)

# Graficul consumului de gaz pentru incalzire pe locatii si state
fig_gas = px.bar(df, x='Location', y='total_gas_heating', color='State', title='Consumul total de gaz pentru incalzire pe locatii si state')
fig_gas.show()

# Graficul consumului de electricitate pentru incalzire pe locatii si state
fig_electric = px.bar(df, x='Location', y='total_electric_heating', color='State', title='Consumul total de electricitate pentru incalzire pe locatii si state')
fig_electric.show()
