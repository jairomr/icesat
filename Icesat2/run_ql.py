import os
import tempfile
from datetime import datetime
from multiprocessing import Pool
from random import randint
from time import sleep

import geopandas as gpd
import numpy as np
import pandas as pd
from icesat2.config import logger, settings
from icesat2.db import engine
from icesat2.model.atl_ql import Base
from icesat2.nasa_login import SessionWithHeaderRedirection
from icesat2.utils import process_atl03, process_atl08
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from requests import Session
from rich import print

from icesat2.function import geohash_lapig

Base.metadata.create_all(engine)


def savefile(args):
    url, _id, session, error = args
    tstart = datetime.now()
    sleep(randint(error, (2 + (error * 10))))
    session = SessionWithHeaderRedirection(
        settings.username, settings.password
    )
    namefile_atl8 = url.split('/')[-1].replace('QL', '')
    try:
        logger.info(f'Tentado baixar: {_id}')

        logger.debug(url)

        f_atl08 = session.get(url, allow_redirects=True)

        with tempfile.TemporaryDirectory() as tmpdirname:
            if f_atl08.ok:
                logger.info(f'Ok Baixando {namefile_atl8}')   # Say
                with tempfile.TemporaryDirectory() as tmpdirname:
                    file_name8 = f'{tmpdirname}/{namefile_atl8}'

                    with open(file_name8, 'wb') as f:
                        f.write(f_atl08.content)

                    file_stats8 = os.stat(file_name8).st_size

                    df8 = process_atl08(file_name8)

                    atl8_len = len(df8)
                    atl8_len_geohash = 0

                    if atl8_len > 0:
                        df8 = geohash_lapig(df8)
                        atl8_len_geohash = len(df8)

                    if atl8_len_geohash > 0:
                        gdf8 = gpd.GeoDataFrame(
                            df8,
                            crs=4326,
                            geometry=gpd.points_from_xy(
                                df8['longitude'], df8['latitude']
                            ),
                        )
                        gdf8 = gdf8.drop(columns=['longitude', 'latitude'])

                        gdf8['_id'] = _id

                        gdf8['_id'] = gdf8['_id'].astype(np.int32)

                        gdf8.to_postgis(
                            settings.DB_NAME_ATL8,
                            engine,
                            if_exists='append',
                            index=False,
                        )

                        tend = datetime.now()
                        tempo_gasto = tend - tstart
                        with MongoClient(
                            f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
                        ) as client:

                            db = client['icesat2']
                            collection = db[settings.COLLECTION_NAME]

                            doc = {
                                '_id': _id,
                                'file': namefile_atl8,
                                'url': url,
                                'status': 'downloaded',
                                'size': {
                                    'atl8': file_stats8,
                                },
                                'len': {
                                    'atl8': atl8_len,
                                    'atl8_hash': atl8_len_geohash,
                                },
                                'time': {'start': tstart, 'end': tend},
                                'tempogasto': tempo_gasto.total_seconds() / 60,
                            }

                            try:
                                collection.insert_one(doc)
                                logger.success(f'Foi salvo o {_id}')
                            except DuplicateKeyError:
                                # Se ocorrer a exceção de chave duplicada, atualize o documento existente
                                collection.update_one(
                                    {'_id': _id}, {'$set': doc}
                                )
                                logger.success(
                                    f'Documento {_id} atualizado com sucesso.'
                                )

                    else:
                        tend = datetime.now()
                        tempo_gasto = tend - tstart
                        with MongoClient(
                            f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
                        ) as client:
                            db = client['icesat2']
                            collection = db[settings.COLLECTION_NAME]

                            doc = {
                                '_id': _id,
                                'file': namefile_atl8,
                                'url': url,
                                'status': 'empty file',
                                'size': {
                                    'atl8': file_stats8,
                                },
                                'len': {
                                    'atl8': atl8_len,
                                    'atl8_hash': atl8_len_geohash,
                                },
                                'time': {'start': tstart, 'end': tend},
                                'tempogasto': tempo_gasto.total_seconds() / 60,
                            }
                            try:
                                collection.insert_one(doc)
                            except DuplicateKeyError:
                                # Se ocorrer a exceção de chave duplicada, atualize o documento existente
                                collection.update_one(
                                    {'_id': _id}, {'$set': doc}
                                )

                            logger.warning(f'Esta vazio {_id}')
            elif (f_atl08.status_code == 503) and error < 10:
                logger.debug(f'ERROS:{error} - {namefile_atl8}')
                savefile((url, _id, session, error + 1))
            else:
                logger.debug(f_atl08.text)
                logger.warning(f'Erro ao baixar {f_atl08.status_code}')

    except Exception as e:
        logger.exception('Error not mapeado')
        with MongoClient(
            f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
        ) as client:
            db = client['icesat2']
            collection = db[settings.COLLECTION_NAME]
            doc = {
                '_id': _id,
                'file': namefile_atl8,
                'url': url,
                'status': 'error',
                'msg': str(e),
            }

            try:
                collection.insert_one(doc)
                logger.warning(f'Foi salvo o {_id}')
            except DuplicateKeyError:
                collection.update_one({'_id': _id}, {'$set': doc})
                logger.warning(
                    f'Documento com erro {_id} atualizado com sucesso.'
                )


if __name__ == '__main__':
    with MongoClient(
        f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
    ) as client:
        db = client['icesat2']
        collection = db[settings.COLLECTION_NAME]

        files_runs = collection.find(
            {'$or': [{'status': 'downloaded'}, {'status': 'empty file'}]}
        ).distinct('file')

    df = pd.read_csv('urls.dat')
    df['file'] = df['url'].apply(lambda x: x.split('/')[-1].replace('QL', ''))
    total = len(df)
    df = df[~df['file'].isin(files_runs)]
    complet = len(df)
    logger.info(f'Feito {total-complet} de {total} falta {complet}')

    # savefile((df['url'].iloc[0],session))
    with Session() as session:
        session.auth = (settings.username, settings.password)
        args = [
            (row.url, row._id, 'session', 0)
            for index, row in df[['url', '_id']].iterrows()
        ]
        with Pool(settings.CORE) as works:
            works.map(savefile, args)
