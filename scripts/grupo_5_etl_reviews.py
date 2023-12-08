# libraries
import pandas as pd
import csv
from datetime import datetime
from bs4 import BeautifulSoup # Para eliminar html de description
import swifter
import fasttext

# Descargar antes el modelo pre-entrenado
# wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz
model = fasttext.load_model('lid.176.ftz')

# Función para detectar idioma utilizando fastText
def detect_language_fasttext(text):
    if pd.isna(text) or text.strip() == '':
        return "unknown"
    else:
        # FastText espera una lista de textos para la predicción y devuelve una lista de tuplas
        predictions = model.predict([text])
        # El primer elemento de la tupla es el código de idioma con el prefijo '__label__'
        language = predictions[0][0][0].replace('__label__', '')
        return language

def etl_flow_reviews():
    # 1. Leer archivo reviews
    print("***** Leyendo archivo reviews")
    reviews_detailed = pd.read_csv('data/reviews.csv.gz')

    #2. Transformaciones
    print("***** Transformando datos")

    # Eliminar filas con 'comments' vacíos
    reviews_detailed = reviews_detailed.dropna(subset=['comments'])

    # Convertir fechas a datetime
    reviews_detailed['date'] = pd.to_datetime(reviews_detailed['date'])

    # Eliminar campos reviewer_id y reviewer_name
    reviews_detailed = reviews_detailed.drop(['reviewer_id', 'reviewer_name'], axis=1)

    # Remover etiquetas HTML de 'comments'
    print("***** Eliminando etiquetas HTML en comments")
    reviews_detailed['comments'] = reviews_detailed['comments'].apply(lambda x: BeautifulSoup(x, 'html.parser').get_text() if pd.notna(x) else x)

    # 3. Agregar columna con el idioma de la reseña
    print("***** Agregando columna con el idioma de la reseña")
    reviews_detailed['language'] = reviews_detailed['comments'].swifter.apply(detect_language_fasttext)

    #6. Guardar archivo
    print("***** Guardando archivo")
    reviews_detailed.to_csv('data/reviews_summary.csv', index=False, quoting=csv.QUOTE_ALL, escapechar="\\")

if __name__ == "__main__":
    etl_flow_reviews()
    print("***** Archivo guardado, listo para subir a AWS :) *****")