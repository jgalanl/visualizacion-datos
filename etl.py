from http import HTTPStatus
import gzip
import requests
import os
import pandas as pd
import json
import numpy as np


from sqlalchemy import create_engine

GASOLINERAS_URL = 'https://geoportalgasolineras.es/resources/files/preciosEESS_es.xls'


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


def extraction():
    '''
    Download data from GitHub repository
    '''
    os.makedirs('data/extraction', exist_ok=True)
    download(GASOLINERAS_URL, 'gasolineras.xls')


def transformation():
    '''
    Clean all data
    '''
    os.makedirs('data/transformation', exist_ok=True)
    df = pd.read_excel(
        'data/extraction/gasolineras.xls',
        header=0,
        engine='xlrd',
        skiprows=3,
        decimal=',',
        dtype={'Precio gasolina 95 E5': np.float32,
               'Longitud': np.float32,
               'Latitud': np.float32}
    )
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
        data.to_sql('test', con=connection,
                    index=False, if_exists='append')


def etl():
    # extraction()
    data_transformed = transformation()
    load(data_transformed)


if __name__ == "__main__":
    etl()
