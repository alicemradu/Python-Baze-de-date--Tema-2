from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)

@app.route('/api/v1/resources/an_fabricatie', methods=['GET'])
def get_daune_by_an_fabricatie():
    # Preluarea parametrului an_fabricatie din cerere
    an_fabricatie = request.args.get('an_fabricatie')
    if not an_fabricatie:
        return jsonify({"error": "Missing parameter: an_fabricatie"}), 400

    # Verificarea ca an_fabricatie este un numar intreg
    try:
        an_fabricatie = int(an_fabricatie)
    except ValueError:
        return jsonify({"error": "Invalid parameter: an_fabricatie must be an integer"}), 400

    # Conectarea la MongoDB
    client = MongoClient("mongodb://master:stud1234@193.226.34.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
    db = client['daune_leasing']
    collection = db['clienti_daune']

    # Definirea proiectiei si sortarii
    projection = {
        "_id": 0,
        "AN_FABRICATIE": 1,
        "MARCA": 1,
        "VALOARE_DAUNA": 1,
        "ID_CLIENT": 1
    }
    sort = [("MARCA", -1)]

    # Interogarea bazei de date
    cursor = collection.find({"AN_FABRICATIE": an_fabricatie}, projection).sort(sort)
    daune_list = list(cursor)

    return jsonify(daune_list), 200

if __name__ == '__main__':
    app.run(debug=True)
