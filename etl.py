from http import HTTPStatus
import gzip
import requests
import os
import pandas as pd
import json


from sqlalchemy import create_engine

HEADERS_CONVOCATORIAS_URL = 'https://raw.githubusercontent.com/JaimeObregon/subvenciones/main/db/mongodb/data/convocatorias-headers.csv'
CONVOCATORIAS_URL = 'https://raw.githubusercontent.com/JaimeObregon/subvenciones/main/files/convocatorias.csv.gz'
HEADERS_JURIDICAS_URL = 'https://raw.githubusercontent.com/JaimeObregon/subvenciones/main/db/mongodb/data/juridicas-headers.csv'
JURIDICAS1_URL = 'https://raw.githubusercontent.com/JaimeObregon/subvenciones/main/files/juridicas_1.csv.gz'
JURIDICAS2_URL = 'https://raw.githubusercontent.com/JaimeObregon/subvenciones/main/files/juridicas_2.csv.gz'


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
    download(HEADERS_CONVOCATORIAS_URL, 'headers_convocatorias.csv')
    download(CONVOCATORIAS_URL, 'convocatorias.csv.gz')
    unzip('convocatorias.csv.gz', 'convocatorias.csv')
    download(HEADERS_JURIDICAS_URL, 'headers_juridicas.csv')
    download(JURIDICAS1_URL, 'juridicas1.csv.gz')
    unzip('juridicas1.csv.gz', 'juridicas1.csv')
    download(JURIDICAS2_URL, 'juridicas2.csv.gz')
    unzip('juridicas2.csv.gz', 'juridicas2.csv')


def transformation():
    '''
    Clean all data
    '''
    os.makedirs('data/transformation', exist_ok=True)
    names = pd.read_csv(
        'data/extraction/headers_convocatorias.csv', skipinitialspace=True)

    df = pd.read_csv('data/extraction/convocatorias.csv.gz', header=None, names=list(names),  skipinitialspace=True,
                     compression='gzip', engine='python', encoding='utf-8', low_memory=True, memory_map=True)

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
