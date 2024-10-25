from flask import Flask, jsonify, request
import pandas as pd
from datetime import datetime
import holidays as hd
import os
import joblib
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


@app.route('/fechas', methods=['POST'])
def procesar_datos():

    data = request.get_json()
    fecha_inicio = data.get('fecha_inicio')
    fecha_fin = data.get('fecha_fin')
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, '%d-%m-%Y')
        fecha_fin = datetime.strptime(fecha_fin, '%d-%m-%Y')
    except ValueError:
        return jsonify({"error": "El formato de las fechas debe ser 'dd-mm-yyyy'"}), 400
    
    rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin)
    fechas = [fecha.strftime('%d-%m-%Y') for fecha in rango_fechas]
    

    personal_area_1 = pd.read_csv('data/processed/areas/Personal_5M7.csv')
    personal_area_2 = pd.read_csv('data/processed/areas/Personal_5M23.csv')
    personal_area_3 = pd.read_csv('data/processed/areas/Personal_5M24.csv')
    personal_area_4 = pd.read_csv('data/processed/areas/Personal_5M41.csv')

    personal_area_1['Areas_M/G-5M23'] = False
    personal_area_1['Areas_M/G-5M24'] = False
    personal_area_1['Areas_M/G-5M41'] = False
    personal_area_1['Areas_M/G-5M7'] = True
    personal_area_2['Areas_M/G-5M23'] = True
    personal_area_2['Areas_M/G-5M24'] = False
    personal_area_2['Areas_M/G-5M41'] = False
    personal_area_2['Areas_M/G-5M7'] = False
    personal_area_3['Areas_M/G-5M23'] = False
    personal_area_3['Areas_M/G-5M24'] = True
    personal_area_3['Areas_M/G-5M41'] = False
    personal_area_3['Areas_M/G-5M7'] = False
    personal_area_4['Areas_M/G-5M23'] = False
    personal_area_4['Areas_M/G-5M24'] = False
    personal_area_4['Areas_M/G-5M41'] = True
    personal_area_4['Areas_M/G-5M7'] = False
    personal_area_1_limpio = personal_area_1.drop(columns=['Contrato_Contrato Temporal.1', 'Contrato_Indeterminado.1'])
    personal_area_2_limpio = personal_area_2.drop(columns=['Contrato_Contrato Temporal.1', 'Contrato_Indeterminado.1'])
    personal_area_3_limpio = personal_area_3.drop(columns=['Contrato_Contrato Temporal.1', 'Contrato_Indeterminado.1'])
    personal_area_4_limpio = personal_area_4.drop(columns=['Contrato_Contrato Temporal.1', 'Contrato_Indeterminado.1'])
    personal_area_1_limpio['centro'] = 6334460
    personal_area_2_limpio['centro'] = 6334460
    personal_area_3_limpio['centro'] = 6334460
    personal_area_4_limpio['centro'] = 6334460
    carpeta = 'data/predict'
    for archivo in os.listdir(carpeta):
        ruta_archivo = os.path.join(carpeta, archivo)
        if os.path.isfile(ruta_archivo):
            os.remove(ruta_archivo)
            print(f"Archivo eliminado: {ruta_archivo}")

    for fecha in fechas:
        # Crear una variable de tipo datetime
        current_date = datetime.strptime(fecha, '%d-%m-%Y')
        # current_date = datetime.now().date()
        
        # Agregar la columna 'Date' a los DataFrames
        personal_area_1_limpio['Date'] = current_date
        personal_area_2_limpio['Date'] = current_date
        personal_area_3_limpio['Date'] = current_date
        personal_area_4_limpio['Date'] = current_date
        column_order = [
            'ID', 'centro', 'Rotacion', 'Edad', 'Hijos', 'año_contrato',
            'Contrato_Contrato Temporal', 'Contrato_Indeterminado',
            'Funcion_Go-Electric', 'Funcion_Portavoz', 'Funcion_Representante Sindical',
            'Funcion_Trabajador', 'Genero_Femenino', 'Genero_Masculino',
            'Turno_R6D__01', 'Turno_RG1_2T', 'Turno_RG1_3T', 'Turno_RG2_2T', 'Turno_RG2_3T',
            'Turno_RG3_3T', 'Turno_T_CENT', 'Areas_M/G-5M23', 'Areas_M/G-5M24',
            'Areas_M/G-5M41', 'Areas_M/G-5M7', 'Date'
        ]
        
        personal_area_1_limpio = personal_area_1_limpio[column_order]
        personal_area_2_limpio = personal_area_2_limpio[column_order]
        personal_area_3_limpio = personal_area_3_limpio[column_order]
        personal_area_4_limpio = personal_area_4_limpio[column_order]
        # Convertir la columna 'Date' a datetime
        personal_area_1_limpio['Date'] = pd.to_datetime(personal_area_1_limpio['Date'])
        personal_area_2_limpio['Date'] = pd.to_datetime(personal_area_2_limpio['Date'])
        personal_area_3_limpio['Date'] = pd.to_datetime(personal_area_3_limpio['Date'])
        personal_area_4_limpio['Date'] = pd.to_datetime(personal_area_4_limpio['Date'])
        
        # Crear las nuevas columnas
        personal_area_1_limpio['Year'] = personal_area_1_limpio['Date'].dt.year
        personal_area_1_limpio['Month'] = personal_area_1_limpio['Date'].dt.month
        personal_area_1_limpio['Day'] = personal_area_1_limpio['Date'].dt.day
        personal_area_1_limpio['Week Day'] = personal_area_1_limpio['Date'].dt.dayofweek
        
        personal_area_2_limpio['Year'] = personal_area_2_limpio['Date'].dt.year
        personal_area_2_limpio['Month'] = personal_area_2_limpio['Date'].dt.month
        personal_area_2_limpio['Day'] = personal_area_2_limpio['Date'].dt.day
        personal_area_2_limpio['Week Day'] = personal_area_2_limpio['Date'].dt.dayofweek
        
        personal_area_3_limpio['Year'] = personal_area_3_limpio['Date'].dt.year
        personal_area_3_limpio['Month'] = personal_area_3_limpio['Date'].dt.month
        personal_area_3_limpio['Day'] = personal_area_3_limpio['Date'].dt.day
        personal_area_3_limpio['Week Day'] = personal_area_3_limpio['Date'].dt.dayofweek
        
        personal_area_4_limpio['Year'] = personal_area_4_limpio['Date'].dt.year
        personal_area_4_limpio['Month'] = personal_area_4_limpio['Date'].dt.month
        personal_area_4_limpio['Day'] = personal_area_4_limpio['Date'].dt.day
        personal_area_4_limpio['Week Day'] = personal_area_4_limpio['Date'].dt.dayofweek
        
        # Eliminar filas duplicadas de cada DataFrame
        personal_area_1_final = personal_area_1_limpio.drop_duplicates()
        personal_area_2_final = personal_area_2_limpio.drop_duplicates()
        personal_area_3_final = personal_area_3_limpio.drop_duplicates()
        personal_area_4_final = personal_area_4_limpio.drop_duplicates()
        
        festivos = hd.Mexico(years=[2022, 2023, 2024])
        personal_area_1_final['Date'] = pd.to_datetime(personal_area_1_final['Date'])
        personal_area_1_final['HOLIDAY'] = personal_area_1_final['Date'].apply(lambda x: x in festivos)
        
        personal_area_2_final['Date'] = pd.to_datetime(personal_area_2_final['Date'])
        personal_area_2_final['HOLIDAY'] = personal_area_2_final['Date'].apply(lambda x: x in festivos)
        
        personal_area_3_final['Date'] = pd.to_datetime(personal_area_3_final['Date'])
        personal_area_3_final['HOLIDAY'] = personal_area_3_final['Date'].apply(lambda x: x in festivos)
        
        personal_area_4_final['Date'] = pd.to_datetime(personal_area_4_final['Date'])
        personal_area_4_final['HOLIDAY'] = personal_area_4_final['Date'].apply(lambda x: x in festivos)
        
        with open('data/dates/soccer_dates.json', 'r') as f:
            soccer_dates = json.load(f)
        # Extract and convert the dates to `date` objects
        soccer_dates = soccer_dates['match_dates']
        soccer_dates = [datetime.strptime(d, "%d/%m/%y") for d in soccer_dates]
        
        personal_area_1_final['SOCCER'] = personal_area_1_final['Date'].apply(lambda x: x in soccer_dates)
        personal_area_2_final['SOCCER'] = personal_area_2_final['Date'].apply(lambda x: x in soccer_dates)
        personal_area_3_final['SOCCER'] = personal_area_3_final['Date'].apply(lambda x: x in soccer_dates)
        personal_area_4_final['SOCCER'] = personal_area_4_final['Date'].apply(lambda x: x in soccer_dates)
        
        with open('data/dates/religious_dates.json', 'r') as f:
            religious_dates = json.load(f)
        
        # Extract and convert the dates to `date` objects
        religious_dates = religious_dates['religious_dates']
        religious_dates = [datetime.strptime(d, "%d/%m/%y") for d in religious_dates]
        
        personal_area_1_final['RELIGIOUS'] = personal_area_1_final['Date'].apply(lambda x: x in religious_dates)
        personal_area_2_final['RELIGIOUS'] = personal_area_2_final['Date'].apply(lambda x: x in religious_dates)
        personal_area_3_final['RELIGIOUS'] = personal_area_3_final['Date'].apply(lambda x: x in religious_dates)
        personal_area_4_final['RELIGIOUS'] = personal_area_4_final['Date'].apply(lambda x: x in religious_dates)
        
        with open('data/dates/concert_dates.json', 'r') as f:
            concert_dates = json.load(f)
        
        # Extract and convert the dates to `date` objects
        concert_dates = concert_dates['concert_dates']
        concert_dates = [datetime.strptime(d, "%d/%m/%y") for d in concert_dates]
        
        personal_area_1_final['CONCERT'] = personal_area_1_final['Date'].apply(lambda x: x in concert_dates)
        personal_area_2_final['CONCERT'] = personal_area_2_final['Date'].apply(lambda x: x in concert_dates)
        personal_area_3_final['CONCERT'] = personal_area_3_final['Date'].apply(lambda x: x in concert_dates)
        personal_area_4_final['CONCERT'] = personal_area_4_final['Date'].apply(lambda x: x in concert_dates)
        
        with open('data/dates/fair_dates.json', 'r') as f:
            fair_dates = json.load(f)
        
        # Extract and convert the dates to `date` objects
        fair_dates = fair_dates['fair_dates']
        fair_dates = [datetime.strptime(d, "%d/%m/%y") for d in fair_dates]
        
        personal_area_1_final['FAIR'] = personal_area_1_final['Date'].apply(lambda x: x in fair_dates)
        personal_area_2_final['FAIR'] = personal_area_2_final['Date'].apply(lambda x: x in fair_dates)
        personal_area_3_final['FAIR'] = personal_area_3_final['Date'].apply(lambda x: x in fair_dates)
        personal_area_4_final['FAIR'] = personal_area_4_final['Date'].apply(lambda x: x in fair_dates)
        
        personal_area_1_final['Evento'] = personal_area_1_final['HOLIDAY'] | personal_area_1_final['SOCCER'] | \
                                          personal_area_1_final['RELIGIOUS'] | personal_area_1_final['CONCERT'] | \
                                          personal_area_1_final['FAIR']
        
        personal_area_2_final['Evento'] = personal_area_2_final['HOLIDAY'] | personal_area_2_final['SOCCER'] | \
                                          personal_area_2_final['RELIGIOUS'] | personal_area_2_final['CONCERT'] | \
                                          personal_area_2_final['FAIR']
        
        personal_area_3_final['Evento'] = personal_area_3_final['HOLIDAY'] | personal_area_3_final['SOCCER'] | \
                                          personal_area_3_final['RELIGIOUS'] | personal_area_3_final['CONCERT'] | \
                                          personal_area_3_final['FAIR']
        
        personal_area_4_final['Evento'] = personal_area_4_final['HOLIDAY'] | personal_area_4_final['SOCCER'] | \
                                          personal_area_4_final['RELIGIOUS'] | personal_area_4_final['CONCERT'] | \
                                          personal_area_4_final['FAIR']
        
        personal_area_1_final.drop(columns=['HOLIDAY', 'SOCCER', 'RELIGIOUS', 'CONCERT', 'FAIR'], inplace=True)
        personal_area_2_final.drop(columns=['HOLIDAY', 'SOCCER', 'RELIGIOUS', 'CONCERT', 'FAIR'], inplace=True)
        personal_area_3_final.drop(columns=['HOLIDAY', 'SOCCER', 'RELIGIOUS', 'CONCERT', 'FAIR'], inplace=True)
        personal_area_4_final.drop(columns=['HOLIDAY', 'SOCCER', 'RELIGIOUS', 'CONCERT', 'FAIR'], inplace=True)
        df1 = personal_area_1_final.drop(columns=['Date'], axis=1)
        df2 = personal_area_2_final.drop(columns=['Date'], axis=1)
        df3 = personal_area_3_final.drop(columns=['Date'], axis=1)
        df4 = personal_area_4_final.drop(columns=['Date'], axis=1)
        # Borrar todo lo que este en la carpeta predict
        


        #
        df1.to_csv(f'data/predict/Area_5M7_{fecha}.csv', index=False)
        df2.to_csv(f'data/predict/Area_5M23_{fecha}.csv', index=False)
        df3.to_csv(f'data/predict/Area_5M24_{fecha}.csv', index=False)
        df4.to_csv(f'data/predict/Area_5M41_{fecha}.csv', index=False)

    return jsonify({"message": "Las fechas se cargaron correctamente"}), 200

    


try:
    modelo = joblib.load('model/absenteeism_model.pkl')
except Exception as e:
    print(f"Error al cargar el modelo: {e}")
    modelo = None



@app.route('/predicciones', methods=['GET'])
def obtain_absences():
    # Obtener listas de archivos
    datos_5M7 = [file for file in os.listdir('data/predict') if 'Area_5M7' in file]
    datos_5M23 = [file for file in os.listdir('data/predict') if 'Area_5M23' in file]
    datos_5M24 = [file for file in os.listdir('data/predict') if 'Area_5M24' in file]
    datos_5M41 = [file for file in os.listdir('data/predict') if 'Area_5M41' in file]

    resultados_totales = []

    # Predecir ausencias
    resultados_totales += predict_absences(datos_5M7, 'Área 5M7', 3)
    resultados_totales += predict_absences(datos_5M23, 'Área 5M23', 174)
    resultados_totales += predict_absences(datos_5M24, 'Área 5M24', 646)
    resultados_totales += predict_absences(datos_5M41, 'Área 5M41', 23)

    return jsonify(resultados_totales), 200


def predict_absences(file_list, area_name, factor):
    resultados = []
    for file in file_list:
        df = pd.read_csv(f'data/predict/{file}')
        df.columns = [col.replace('Areas_', 'Area_') for col in df.columns]
        predictions = modelo.predict(df)

        # Extraer la fecha del nombre del archivo
        date = file.split('_')[-1].replace('.csv', '')

        prob_faltas = (sum(predictions) / 10) * factor

        resultado = {
            "area_name": area_name,
            "fecha": date,
            "prob_faltas": prob_faltas
        }

        resultados.append(resultado)

    return resultados
    

    
if __name__ == '__main__':
    app.run(debug=True)
