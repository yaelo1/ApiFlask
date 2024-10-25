import os
from flask import Flask, request, jsonify
import pandas as pd
import joblib
from datetime import datetime

app = Flask(__name__)


UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

modelo_path = 'random_forest_model.pkl'
try:
    modelo = joblib.load(modelo_path)
except Exception as e:
    print(f"Error al cargar el modelo: {e}")
    modelo = None


@app.route('/ausentismo_rango', methods=['POST'])
def ausentismo_rango():
    if modelo is None:
        return jsonify({"error": "Modelo no cargado"}), 500
    
    start_date = request.json.get("start_date")
    end_date = request.json.get("end_date")

    if not start_date or not end_date:
        return jsonify({"error": "Debe proporcionar 'start_date' y 'end_date' en el cuerpo de la solicitud"}), 400
    


    
if __name__ == '__main__':
    app.run(debug=True)
