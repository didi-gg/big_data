# libraries
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup # Para eliminar html de description

# Campos para seleccionar del conjunto de datos
selected_fields = ['id',
'listing_url', # Se elimina al final
'accommodates',
'bedrooms',
'beds',
'description',
'first_review',
'host_acceptance_rate',
'host_is_superhost',
'host_response_rate',
'host_response_time',
'latitude',
'longitude',
'name',
'neighborhood_overview',
'neighbourhood',
'number_of_reviews',
'price',
'property_type',
'review_scores_accuracy',# Se elimina al final
'review_scores_checkin',# Se elimina al final
'review_scores_cleanliness',# Se elimina al final
'review_scores_communication',# Se elimina al final
'review_scores_location',# Se elimina al final
'review_scores_rating',# Se elimina al final
'review_scores_value',# Se elimina al final
'reviews_per_month',
'room_type',
'source']

# Campos para calcular promedio de Scores
cols_scores = ['review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness',
           'review_scores_checkin', 'review_scores_communication', 
           'review_scores_location', 'review_scores_value']

# Lista de columnas a eliminar
cols_to_drop = ['review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness',
                'review_scores_checkin', 'review_scores_communication', 
                'review_scores_location', 'review_scores_value', 'description', 'neighborhood_overview', 'first_review']

def fill_values(row):
    """
    Rellena los valores faltantes en las columnas 'beds' y 'bedrooms' basado en condiciones específicas.

    Parámetros:
    row (pd.Series): Una fila del DataFrame.

    Retorna:
    pd.Series: La fila con valores actualizados en 'beds' y 'bedrooms' si es necesario.
    """
    # Si 'beds' es NaN, 'bedrooms' es 1 y 'accommodates' es 2 o menos, establecer 'beds' a 1
    if pd.isna(row['beds']) and row['bedrooms'] == 1 and row['accommodates'] <= 2:
        row['beds'] = 1
    # Si 'bedrooms' es NaN y 'beds' no es NaN y es 1, establecer 'bedrooms' a 1
    if pd.isna(row['bedrooms']) and not pd.isna(row['beds']) and row['beds'] == 1:
        row['bedrooms'] = 1
    # Si 'beds' es NaN y 'accommodates' es 2 o menos, establecer 'beds' a 1
    if pd.isna(row['beds']) and not pd.isna(row['accommodates']) and row['accommodates'] <= 2:
        row['beds'] = 1
    # Si 'bedrooms' es NaN y el tipo de habitación es 'Private room', establecer 'bedrooms' a 1
    if pd.isna(row['bedrooms']) and row['room_type'] == 'Private room':
        row['bedrooms'] = 1
    # Si 'beds' es NaN y 'bedrooms' no es NaN, igualar 'beds' a 'bedrooms'
    if pd.isna(row['beds']) and not pd.isna(row['bedrooms']):
        row['beds'] = row['bedrooms']
    # Si tanto 'beds' como 'bedrooms' son NaN y 'accommodates' no es NaN, establecer 'beds' a 'accommodates'
    if pd.isna(row['beds']) and pd.isna(row['bedrooms']) and not pd.isna(row['accommodates']):
        row['beds'] = row['accommodates']
    # Si 'bedrooms' es NaN, 'beds' y 'accommodates' no son NaN, establecer 'bedrooms' a 'beds'
    if pd.isna(row['bedrooms']) and not pd.isna(row['beds']) and not pd.isna(row['accommodates']):
        row['bedrooms'] = row['beds']
    return row

def etl_flow_listings():
    # 1. Leer archivo listings
    print("***** Leyendo archivo listings")
    listings_detailed = pd.read_csv('data/listings.csv.gz')

    #2. Seleccionar campos
    df_listings_summary = listings_detailed[selected_fields]

    #3. Transformaciones
    print("***** Transformando datos")
    # Quitar porcentajes y convertir a número:
    df_listings_summary['host_response_rate'] = df_listings_summary['host_response_rate'].str.rstrip('%').astype('float') / 100
    df_listings_summary['host_acceptance_rate'] = df_listings_summary['host_acceptance_rate'].str.rstrip('%').astype('float') / 100

    # Convertir 'host_is_superhost' a boolean
    df_listings_summary['host_is_superhost'] = df_listings_summary['host_is_superhost'].map({'f': False, 't': True})

    # Quitar signo de moneda en 'price'
    df_listings_summary['price'] = df_listings_summary['price'].replace('[\$,]', '', regex=True).astype(float)

    # Promedio de los Scores
    df_listings_summary['average_score'] = df_listings_summary[cols_scores].mean(axis=1).round(2)

    # Convertir fechas a datetime
    df_listings_summary['first_review'] = pd.to_datetime(df_listings_summary['first_review'])

    # Calcular meses desde el primer review hasta la fecha de corte de la base de datos
    df_listings_summary['first_review'] = pd.to_datetime(df_listings_summary['first_review'])

    fecha_corte = datetime(2023, 9, 4) # Fecha de corte

    # Calcular la diferencia en meses
    df_listings_summary['months_since_first_review'] = df_listings_summary['first_review'].apply(lambda x: (fecha_corte.year - x.year) * 12 + fecha_corte.month - x.month if pd.notna(x) else None)

    # Remover etiquetas HTML de 'description' y 'neighborhood_overview'
    print("***** Eliminando etiquetas HTML en description")
    df_listings_summary['description_clean'] = df_listings_summary['description'].apply(lambda x: BeautifulSoup(x, 'html.parser').get_text() if pd.notna(x) else x)
    print("Eliminando etiquetas HTML en neighborhood_overview")
    df_listings_summary['neighborhood_overview_clean'] = df_listings_summary['neighborhood_overview'].apply(lambda x: BeautifulSoup(x, 'html.parser').get_text() if pd.notna(x) else x)

    #4. Manejo de vacíos
    print("***** Llenando vacíos en beds y bedrooms")
    df_listings_summary = df_listings_summary.apply(fill_values, axis=1)

    #5. Eliminar columnas
    print("***** Eliminando columnas")
    df_listings_summary = df_listings_summary.drop(cols_to_drop, axis=1)

    #6. Guardar archivo
    print("***** Guardando archivo")
    df_listings_summary.to_csv('data/listings_summary.csv', index=False)

if __name__ == "__main__":
    etl_flow_listings()
    print("***** Archivo guardado, listo para subir a AWS :) *****")