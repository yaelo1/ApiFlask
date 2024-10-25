import os
from flask import Flask, request, jsonify
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from datetime import datetime

app = Flask(__name__)

#VERIFICAR QUE EXISTA LA CARPETA UPLOADS
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

csv_filename = None

#CARGA DEL MODELO
modelo_path = 'random_forest_model.pkl'
try:
    modelo = joblib.load(modelo_path)
except Exception as e:
    print(f"Error al cargar el modelo: {e}")
    modelo = None


#CARGA DE ARCHIVOS Y NOMBRAMIENTO CON EL TIMESTAMP PARA DIFERENCIARLOS
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No se encontró el archivo en la solicitud"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Nombre de archivo vacío"}), 400
    try:   
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{file.filename}"
        csv_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(csv_path)
        return jsonify({"message": f"Archivo CSV '{filename}' cargado y guardado exitosamente"}), 200
    except Exception as e:
        return jsonify({"error": f"Error al procesar el archivo: {e}"}), 500

#VER ARCHIVOS 
@app.route('/listar_archivos', methods=['GET'])
def listar_archivos():
    try:
        archivos = os.listdir(UPLOAD_FOLDER)
        if not archivos:
            return jsonify({"message": "No hay archivos disponibles"}), 404
        return jsonify({"archivos": archivos}), 200
    except Exception as e:
        return jsonify({"error": f"Error al listar archivos: {e}"}), 500

#SELECCIONAR ARCHIVOS
@app.route('/seleccionar_archivo', methods=['POST'])
def seleccionar_archivo():
    global csv_filename
    archivo_seleccionado = request.json.get('archivo')

    if not archivo_seleccionado:
        return jsonify({"error": "No se especificó el archivo a cargar"}), 400

    archivo_path = os.path.join(UPLOAD_FOLDER, archivo_seleccionado)
    if not os.path.exists(archivo_path):
        return jsonify({"error": f"El archivo '{archivo_seleccionado}' no existe"}), 404

    csv_filename = archivo_path
    return jsonify({"message": f"Archivo '{archivo_seleccionado}' seleccionado correctamente"}), 200


@app.route('/ausentismo_total', methods=['GET'])
def ausentismo_total():
    global csv_filename, modelo
    if not csv_filename or not os.path.exists(csv_filename):
        return jsonify({"error": "No hay archivo seleccionado o no existe"}), 404

    if modelo is None:
        return jsonify({"error": "Modelo no cargado"}), 500

    try:
        data = pd.read_csv(csv_filename)

        if data.empty:
            return jsonify({"error": "El archivo CSV está vacío o no contiene datos"}), 400

        data.rename(columns={"aÃ±o_contrato": "año_contrato"}, inplace=True)
        
        if 'Falta' not in data.columns:
            return jsonify({"error": "El archivo no contiene la columna 'Falta'"}), 400

        # Separar características (X) y objetivo (y)
        X = data.drop('Falta', axis=1)
        y = data['Falta']

        modelo_features = ['ID', 'Dia especial', 'centro', 'Rotacion', 'Edad', 'Hijos', 'año_contrato',
                           'Contrato_Contrato Temporal', 'Contrato_Indeterminado', 'Funcion_Go-Electric',
                           'Funcion_Portavoz', 'Funcion_Representante Sindical', 'Funcion_Trabajador',
                           'Genero_Femenino', 'Genero_Masculino', 'Turno_R6D__01', 'Turno_RG1_2T',
                           'Turno_RG1_3T', 'Turno_RG2_2T', 'Turno_RG2_3T', 'Turno_RG3_3T', 'Turno_T_CENT',
                           'Area_M/G-5M23', 'Area_M/G-5M24', 'Area_M/G-5M41', 'Area_M/G-5M7', 'Year',
                           'Month', 'Day', 'Week Day']

        # Comprobar si faltan columnas
        missing_cols = set(modelo_features) - set(X.columns)
        if missing_cols:
            return jsonify({"error": f"Faltan las siguientes columnas en los datos: {missing_cols}"}), 400

        # Mantener solo las columnas necesarias para el modelo
        X = X[modelo_features]

        # Realizar la predicción
        y_pred = modelo.predict(X)

        # Agregar la predicción al DataFrame
        data['Falta_Prediccion'] = y_pred

        total_faltas = data['Falta_Prediccion'].sum()

        # Devolver el resultado como JSON
        return jsonify({"total_faltas_predichas": int(total_faltas)})

    except Exception as e:
        return jsonify({"error": f"Error al procesar los datos: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
 