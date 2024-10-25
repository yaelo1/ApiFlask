import os
from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

csv_filename = None

data = pd.DataFrame()


@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No se encontró el archivo en la solicitud"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Nombre de archivo vacío"}), 400

    try:
     
        filename = file.filename
        name, ext = os.path.splitext(filename)

        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

      
        new_filename = f"{name}_{timestamp}{ext}"

       
        csv_path = os.path.join(UPLOAD_FOLDER, new_filename)
        file.save(csv_path)

        return jsonify({"message": f"Archivo CSV '{new_filename}' cargado y guardado exitosamente"}), 200
    except Exception as e:
        return jsonify({"error": f"Error al procesar el archivo: {e}"}), 500


@app.route('/listar_archivos', methods=['GET'])
def listar_archivos():
    try:
       
        archivos = os.listdir(UPLOAD_FOLDER)
        if not archivos:
            return jsonify({"message": "No hay archivos disponibles"}), 404
        
        return jsonify({"archivos": archivos}), 200
    except Exception as e:
        return jsonify({"error": f"Error al listar archivos: {e}"}), 500


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


@app.route('/ausentismo_area_M23', methods=['GET'])
def ausentismo_area_23():
    global csv_filename
    if not csv_filename or not os.path.exists(csv_filename):
        return jsonify({"error": "No hay archivo seleccionado o no existe"}), 404

    try:
       
        data = pd.read_csv(csv_filename)

        if data.empty:
            return jsonify({"error": "El archivo CSV está vacío o no contiene datos"}), 400

     
        if 'Area_M/G-5M23' not in data.columns or 'Falta' not in data.columns:
            return jsonify({"error": "El archivo no contiene las columnas requeridas"}), 400

       
        ausentismos_por_area = int(data[data['Area_M/G-5M23'] == True]['Falta'].sum())

        return jsonify({"ausentismos_por_area": ausentismos_por_area})
    except Exception as e:
        return jsonify({"error": f"Error al procesar los datos: {e}"}), 500

@app.route('/ausentismo_area_M24', methods=['GET'])
def ausentismo__area_24():
    global csv_filename
    if not csv_filename or not os.path.exists(csv_filename):
        return jsonify({"error": "No hay archivo seleccionado o no existe"}), 404

    try:
     
        data = pd.read_csv(csv_filename)

        if data.empty:
            return jsonify({"error": "El archivo CSV está vacío o no contiene datos"}), 400

        
        if 'Area_M/G-5M23' not in data.columns or 'Falta' not in data.columns:
            return jsonify({"error": "El archivo no contiene las columnas requeridas"}), 400

       
        ausentismos_por_area = int(data[data['Area_M/G-5M24'] == True]['Falta'].sum())

        return jsonify({"ausentismos_por_area": ausentismos_por_area})
    except Exception as e:
        return jsonify({"error": f"Error al procesar los datos: {e}"}), 500

@app.route('/ausentismo_area_M41', methods=['GET'])
def ausentismo_area_41():
    global csv_filename
    if not csv_filename or not os.path.exists(csv_filename):
        return jsonify({"error": "No hay archivo seleccionado o no existe"}), 404

    try:
        
        data = pd.read_csv(csv_filename)

        if data.empty:
            return jsonify({"error": "El archivo CSV está vacío o no contiene datos"}), 400

       
        if 'Area_M/G-5M23' not in data.columns or 'Falta' not in data.columns:
            return jsonify({"error": "El archivo no contiene las columnas requeridas"}), 400

        
        ausentismos_por_area = int(data[data['Area_M/G-5M41'] == True]['Falta'].sum())

        return jsonify({"ausentismos_por_area": ausentismos_por_area})
    except Exception as e:
        return jsonify({"error": f"Error al procesar los datos: {e}"}), 500

@app.route('/ausentismo_area_M7', methods=['GET'])
def ausentismo_area_7():
    global csv_filename
    if not csv_filename or not os.path.exists(csv_filename):
        return jsonify({"error": "No hay archivo seleccionado o no existe"}), 404

    try:
        data = pd.read_csv(csv_filename)

        if data.empty:
            return jsonify({"error": "El archivo CSV está vacío o no contiene datos"}), 400

       
        if 'Area_M/G-5M23' not in data.columns or 'Falta' not in data.columns:
            return jsonify({"error": "El archivo no contiene las columnas requeridas"}), 400

    
        ausentismos_por_area = int(data[data['Area_M/G-5M7'] == True]['Falta'].sum())

        return jsonify({"ausentismos_por_area": ausentismos_por_area})
    except Exception as e:
        return jsonify({"error": f"Error al procesar los datos: {e}"}), 500


if __name__ == '__main__':
    app.run(debug=True)
