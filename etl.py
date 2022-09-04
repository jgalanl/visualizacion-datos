from http import HTTPStatus
import gzip
import requests
import os
import pandas as pd
import json
import numpy as np
from datetime import datetime
import calendar
import locale


from sqlalchemy import create_engine

GASOLINERAS_URL = 'https://geoportalgasolineras.es/resources/files/preciosEESS_es.xls'

locale.setlocale(locale.LC_ALL, 'esp')


def download(url: str, name: str):
    req = requests.get(url)
    if req.status_code == HTTPStatus.OK:
        with open(f'data/extraction/{name}', 'wb') as file:
            file.write(req.content)
            print(f'{name} successfully downloaded')
    else:
        print(f'Unexpected status code: {req.status_code}')


def unzip(path: str, name: str):
    with gzip.open(f'data/extraction/{path}', 'rb') as file:
        file_content = file.read()
        file = open(f'data/extraction/{name}', 'wb')
        file.write(file_content)
        print(f'{name} successfully unzipped')


def extract():
    '''
    Download data from GitHub repository
    '''
    os.makedirs('data/extraction', exist_ok=True)
    download(GASOLINERAS_URL, 'gasolineras.xls')


def transform():
    '''
    Clean and transform data
    '''
    os.makedirs('data/transformation', exist_ok=True)

    df = pd.read_excel(
        'data/extraction/precios0208.xls',
        header=0,
        engine='xlrd',
        nrows=1
    )
    date_obj = datetime.strptime(df.columns[1], '%d/%m/%Y %H:%M').date()
    week_day = calendar.day_name[date_obj.weekday()].title()

    df = pd.read_excel(
        'data/extraction/precios0208.xls',
        header=0,
        engine='xlrd',
        skiprows=3,
        decimal=',',
        dtype={'Provincia': str,
               'Precio gasolina 95 E5': np.float32,
               'Precio gasolina 95 E10': np.float32,
               'Precio gasolina 95 E5 Premium': np.float32,
               'Precio gasolina 98 E5': np.float32,
               'Precio gasolina 98 E10': np.float32,
               'Precio gasóleo A': np.float32,
               'Precio gasóleo Premium': np.float32,
               'Precio gasóleo B': np.float32,
               'Precio gasóleo C': np.float32,
               'Precio bioetanol': np.float32,
               'Precio biodiésel': np.float32,
               'Precio gases licuados del petróleo': np.float32,
               'Precio gas natural comprimido': np.float32,
               'Precio gas natural licuado': np.float32,
               'Precio hidrógeno': np.float32,
               'Longitud': np.float32,
               'Latitud': np.float32}
    )

    df.columns = [
        'provincia',
        'municipio',
        'localidad',
        'cp',
        'direccion',
        'margen',
        'longitud',
        'latitud',
        'toma_datos',
        'gasolina_95_E5',
        'gasolina_95_E10',
        'gasolina_95_E5_premium',
        'gasolina_98_E5',
        'gasolina_98_E10',
        'gasoleo_a',
        'gasoleo_premium',
        'gasoleo_b',
        'gasoleo_c',
        'bioetanol',
        'porcentaje_bioalcohol',
        'biodiesel',
        'porcentaje_ester_metilico',
        'gases_licuados_petroleo',
        'gas_natural_comprimido',
        'gas_natural_licuado',
        'hidrogeno',
        'rotulo',
        'tipo_venta',
        'rem',
        'horario',
        'tipo_servicio'
    ]

    df['dia_semana'] = week_day
    df['dia'] = date_obj

    return df


def load(data):
    '''
    Load data into PostgreSQL database
    '''
    os.makedirs('data/load', exist_ok=True)
    with open('db/credentials.json') as json_file:
        config = json.load(json_file)
    engine = create_engine(
        f'postgresql://{config["POSTGRE_USER_NAME"]}:{config["POSTGRE_PASSWORD"]}@{config["POSTGRE_HOST"]}:{config["POSTGRE_PORT"]}/{config["POSTGRE_DB"]}')

    with engine.begin() as connection:
        data.to_sql('combustibles', con=connection,
                    index=False, if_exists='append')


def etl():
    # extraction()
    data_transformed = transform()
    load(data_transformed)
    print('ETL finished successfully')


if __name__ == "__main__":
    etl()
