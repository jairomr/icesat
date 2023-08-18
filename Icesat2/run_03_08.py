import os
import tempfile
from datetime import datetime
from multiprocessing import Pool

import geopandas as gpd
import pandas as pd
from icesat2.config import logger, settings
from icesat2.db import engine
from icesat2.function import atl82atl3, geohash_lapig, saveMongo
from icesat2.model.atl import Base
from icesat2.nasa_login import SessionWithHeaderRedirection
from sqlalchemy.orm import sessionmaker
from icesat2.utils import process_atl03, process_atl08
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from requests import Session

Base.metadata.create_all(engine)


def savefile(args):
    url, _id, session, error = args
    tstart = datetime.now()
    error_in_save = False
    code_status = {
        'atl8':False,
        'atl3':False,
        'atl3_pages':{
            'pages':[],
            'number_page':0,
        }
    } 
    
    with MongoClient( f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/') as client:
        db = client['icesat2']
        collection = db['icesat2v9']
        cd_s = collection.find_one({'_id':_id},{'code_status':1})
        try:
            code_status = cd_s['code_status']
            logger.debug(code_status)
        except:
            logger.info('not code status')
            ...            


    session = SessionWithHeaderRedirection(
        settings.username, settings.password
    )
    namefile_atl8 = url.split('/')[-1].replace('QL', '')
    namefile_atl3 = atl82atl3(url).split('/')[-1].replace('QL', '')
    pre_doc = {
        '_id': _id,
        'file': namefile_atl8,
        'url': url,
        'size': {
            'atl8': 0,
            'atl3': 0,
        }
     }
    
    try:
        logger.info(f'Tentado baixar: {namefile_atl8} {namefile_atl3}')

        logger.debug(url)

        f_atl08 = session.get(url, allow_redirects=True)
        

        with tempfile.TemporaryDirectory() as tmpdirname:
            if f_atl08.ok:
                logger.info(
                    f'Ok Baixando {namefile_atl8} {namefile_atl3}'
                )   # Say
                with tempfile.TemporaryDirectory() as tmpdirname:
                    file_name8 = f'{tmpdirname}/{namefile_atl8}'
                    file_name3 = f'{tmpdirname}/{namefile_atl3}'

                    with open(file_name8, 'wb') as f:
                        f.write(f_atl08.content)
                        
                    pre_doc['size']['atl8'] = os.stat(file_name8).st_size
                    logger.info(f'baixnado {file_name8}')
                    
                    df8 = process_atl08(file_name8)
                    logger.info(f'finalizado processamento {file_name8}')
                    
                    atl8_len = 0
                    atl3_len = 0
                    
                    atl8_len = len(df8)
                    
                    atl3_len_geohash = 0
                    if atl8_len > 0:
                        df8 = geohash_lapig(df8)
                        atl8_len_geohash = len(df8)
                        if atl8_len_geohash > 0:
                            logger.info(f'temos {atl8_len_geohash} pontos para atl8')
                            f_atl03 = session.get(atl82atl3(url), allow_redirects=True)
                            with open(file_name3, 'wb') as f:
                                f.write(f_atl03.content)
                            logger.info(f'baixnado {file_name3}')
                            pre_doc['size']['atl3'] = os.stat(file_name3).st_size
                            logger.info(f'processando {file_name3} e {file_name8}')
                            df3 = process_atl03(file_name3, file_name8)
                            logger.info(f'finalizado processamento {file_name3}')
                            atl3_len = len(df3)
                            
                            df3 = geohash_lapig(df3)
                            atl3_len_geohash = len(df3)
                            logger.info(f'temos {atl3_len_geohash} pontos para atl3')
                            logger.info(f'gerado geohash para {file_name3}')

                    

                        

                    if atl8_len_geohash > 0 and atl3_len_geohash > 0:
                        
                        if not code_status['atl8']:
                            gdf8 = gpd.GeoDataFrame(
                                df8,
                                crs=4326,
                                geometry=gpd.points_from_xy(
                                    df8['longitude'], df8['latitude']
                                ),
                            )
                            gdf8 = gdf8.drop(columns=['longitude', 'latitude'])

                            gdf8['_id'] = _id

                            gdf8.to_postgis(
                                settings.DB_NAME_ATL8,
                                engine,
                                if_exists='append',
                                index=False,
                            )
                            code_status['atl8'] = True
                            saveMongo({
                                '_id':_id,
                                'code_status':code_status,
                            })
                        logger.info(f'at8 {file_name8} salvo no banco')

                        gdf3 = gpd.GeoDataFrame(
                            df3,
                            crs=4326,
                            geometry=gpd.points_from_xy(
                                df3['lon_ph'], df3['lat_ph']
                            ),
                        )
                        
                        
                        gdf3 = gdf3.drop(columns=['lon_ph', 'lat_ph'])

                        gdf3['_id'] = _id
                        
                        
                        
                        if atl3_len_geohash > 1000000:
                            if len(code_status['atl3_pages']['pages']) == 0:
                                tmp = [i for i in range(0, atl3_len_geohash, 1000000)]
                                code_status['atl3_pages']['pages'] = [
                                    (i, tmp[n + 1]) for n, i in enumerate(tmp[:-1])
                                ] + [(tmp[-1], atl3_len_geohash)]
                            pages = code_status['atl3_pages']['pages']
                            logger.debug(f'pages {len(pages)}')
                            for number_page, _data in enumerate(pages):
                                __flag_error_loop__ = False
                                start, end = _data
                                now_number_page = code_status['atl3_pages']['number_page']
                                if number_page >= now_number_page and not not code_status['atl3']:
                                    code_status['atl3_pages']['number_page'] = number_page
                                    logger.debug(f'{namefile_atl3} {start} {end}')
                                    try:
                                        gdf3[start:end].to_postgis(
                                            settings.DB_NAME_ATL3,
                                            engine,
                                            if_exists='append',
                                            index=False,
                                        )
                                    except Exception as e:
                                        __flag_error_loop__ = True
                                        raise Exception(e)
                                    finally:
                                        if __flag_error_loop__:
                                            saveMongo({
                                                '_id':_id,
                                                'code_status':code_status,
                                            })
                                            logger.error('{_id}: Erro ao rodar a page {number_page}')
                                        else:
                                            logger.error('{_id}: Page {number_page} salva')
                                            
                        elif not code_status['atl3']:
                            gdf3.to_postgis(
                                settings.DB_NAME_ATL3,
                                engine,
                                if_exists='append',
                                index=False,
                            )
                            code_status['atl3'] = True
                            saveMongo({'_id':_id,'code_status':code_status,
                                            })
                        
                        code_status['atl3'] = True
                       

                        tend = datetime.now()
                        tempo_gasto = tend - tstart
                        with MongoClient(
                            f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
                        ) as client:

                            db = client['icesat2']
                            collection = db[settings.COLLECTION_NAME]
                            doc = {
                                **pre_doc,
                                'status': 'downloaded',
                                'code_status':code_status,
                                'len': {
                                    'atl8': atl8_len,
                                    'alt3': atl3_len,
                                    'atl8_hash': atl8_len_geohash,
                                    'atl3_hash': atl3_len_geohash
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
                        logger.success(
                            f'Foi salvo o {file_name3} {file_name8}'
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
                                **pre_doc,
                                'status': 'empty file',
                                
                                'len': {
                                    'atl8': atl8_len,
                                    'alt3': atl3_len,
                                    'atl8_hash': atl8_len_geohash,
                                    'atl3_hash': atl3_len_geohash,
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
                            logger.warning(
                                f'Esta vazio {file_name3} {file_name8}'
                            )
            # elif (f_atl08.status_code == 503 and f_atl03.status_code == 503) and error < 10:
            #    logger.debug(f'ERROS:{error} - {namefile_atl8}')
            #    savefile((url, session, error+1))
            else:
                logger.debug(f_atl08.text)
                logger.warning(
                    f'Erro ao baixar {f_atl08.status_code} {f_atl03.status_code}'
                )

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


    