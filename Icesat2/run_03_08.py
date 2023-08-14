from requests import Session
import tempfile

from rich import print
import geopandas as gpd
from icesat2.config import settings, logger
from icesat2.utils import process_atl08, process_atl03
import pandas as pd
from multiprocessing import Pool
from sqlalchemy import create_engine
from icesat2.nasa_login import SessionWithHeaderRedirection
from time import sleep
from random import randint
import os
from datetime import datetime
#
engine = create_engine(
    (
        f'postgresql://{settings.DB_USER}:'
        f'{settings.DB_PASS}@{settings.DB_HOST}'
        f':{settings.DB_PORT}/{settings.DATABASE}'
    )
)
from pymongo import MongoClient
import shapely.geometry



def atl82atl3(name):
    return name.replace('ATL08','ATL03')

def savefile(args):
    url, session, error = args
    tstart = datetime.now()
    #sleep(randint(error, (2+(error*10))))
    session = SessionWithHeaderRedirection(settings.username, settings.password)
    namefile_atl8 = url.split('/')[-1].replace('QL','')
    namefile_atl3 = atl82atl3(url).split('/')[-1].replace('QL','')
    try:
        logger.info(f'Tentado baixar: {namefile_atl8} {namefile_atl3}')
        #r1 = session.request('get', url)
        logger.debug(url)
        
        f_atl08 = session.get(url, allow_redirects=True)
        f_atl03 = session.get(atl82atl3(url), allow_redirects=True)

        with tempfile.TemporaryDirectory() as tmpdirname:   
            if f_atl08.ok and f_atl03.ok:
                logger.info(f'Ok Baixando {namefile_atl8} {namefile_atl3}')   # Say
                with tempfile.TemporaryDirectory() as tmpdirname:
                    file_name8 = f'{tmpdirname}/{namefile_atl8}'
                    file_name3 = f'{tmpdirname}/{namefile_atl3}'

                    with open(file_name8, 'wb') as f:
                        f.write(f_atl08.content)

                    with open(file_name3, 'wb') as f:
                        f.write(f_atl03.content)


                    file_stats8 = os.stat(file_name8).st_size
                    file_stats3 = os.stat(file_name3).st_size

                    df8 = process_atl08(file_name8)
                    df3 = process_atl03(file_name3,file_name8)

                    atl8_len = len(df8)
                    atl3_len = len(df3)

                    if atl8_len > 0 and atl3_len > 0:
                        gdf8 = gpd.GeoDataFrame(
                                df8,
                                crs=4326,
                                geometry=gpd.points_from_xy(
                                    df8['longitude'], df8['latitude']
                                ),
                            )
                        gdf8 = gdf8.drop(columns=['longitude', 'latitude'])

                        gdf8['file'] = namefile_atl8

                        gdf8.to_postgis(
                                'atl8_raw_v2', engine, if_exists='append', index=False
                            )


                        gdf3 = gpd.GeoDataFrame(
                                df3,
                                crs=4326,
                                geometry=gpd.points_from_xy(
                                    df3['lon_ph'], df3['lat_ph']
                                ),
                            )
                        gdf3 = gdf3.drop(columns=['lon_ph', 'lat_ph'])

                        gdf3['file'] = namefile_atl3

                        pages = []
                        if atl3_len > 1000000:
                            tmp = [i for i in range(0,atl3_len,1000000)]
                            pages = [(i, tmp[n+1]) for n,i  in enumerate(tmp[:-1])] + [(tmp[-1],atl3_len)]
                            logger.debug(f'pages {len(pages)}')
                            for start, end in pages:
                                logger.debug(f'{namefile_atl3} {start} {end}')
                                gdf3[start:end].to_postgis(
                                        'atl3_raw_v2', engine, if_exists='append', index=False
                                    )

                        else:
                            gdf3.to_postgis(
                                    'atl3_raw_v2', engine, if_exists='append', index=False
                                )


                        tend = datetime.now()
                        tempo_gasto = tend - tstart
                        with MongoClient(
                                f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
                            ) as client:

                            db = client['icesat2']
                            collection = db['icesat2v4']
                            collection.insert_one(
                                    {
                                        'file': namefile_atl8,
                                        'url': url,
                                        'status': 'downloaded',
                                        'size':{
                                            'atl8':file_stats8,
                                            'atl3':file_stats3
                                        },
                                        'len':{
                                            'atl8':atl8_len,
                                            'alt3':atl3_len,
                                            'pages':pages
                                        },
                                        'time':{
                                            'start':tstart,
                                            'end':tend
                                        },
                                        'tempogasto':tempo_gasto.total_seconds() / 60
                                    }
                                )
                        logger.success(f'Foi salvo o {file_name3} {file_name8}')
                    else:
                        tend = datetime.now()
                        tempo_gasto = tend - tstart
                        with MongoClient(
                                f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
                            ) as client:
                            db = client['icesat2']
                            collection = db['icesat2v4']
                            collection.insert_one(
                                    {
                                        'file': namefile_atl8,
                                        'url': url,
                                        'status': 'empty file',
                                        'size':{
                                            'atl8':file_stats8,
                                            'atl3':file_stats3
                                        },
                                        'len':{
                                            'atl8':atl8_len,
                                            'alt3':atl3_len
                                        },
                                        'time':{
                                            'start':tstart,
                                            'end':tend
                                        },
                                        'tempogasto':tempo_gasto.total_seconds() / 60
                                    }
                                )
                            logger.warning(f'Esta vazio {file_name3} {file_name8}')
            #elif (f_atl08.status_code == 503 and f_atl03.status_code == 503) and error < 10:
            #    logger.debug(f'ERROS:{error} - {namefile_atl8}')
            #    savefile((url, session, error+1))
            else:
                logger.debug(f_atl08.text)
                logger.warning(f'Erro ao baixar {f_atl08.status_code} {f_atl03.status_code}')
                
            

    except Exception as e:
        logger.exception('Error not mapeado')
        with MongoClient(
            f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
        ) as client:
            db = client['icesat2']
            collection = db['icesat2v4']
            collection.insert_one({
                'file': namefile_atl8, 
                'url': url, 
                'status': str(e)}
            
            )


if __name__ == '__main__':
    with MongoClient(
        f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/'
    ) as client:
        db = client['icesat2']
        collection = db['icesat2v4']

        files_runs = collection.find(
            {'$or': [{'status': 'downloaded'}, {'status': 'empty file'}]}
        ).distinct('file')

    df = pd.read_csv('urls.dat')
    df['file'] = df['url'].apply(lambda x: x.split('/')[-1].replace('QL',''))
    total = len(df)
    df = df[~df['file'].isin(files_runs)]
    complet = len(df)
    logger.info(f'Feito {total-complet} de {total} falta {complet}')

    
        # savefile((df['url'].iloc[0],session))
    with Session() as session:
        session.auth = (settings.username, settings.password)
        args = [(url,session,0) for url in df['url']]
        with Pool(settings.CORE) as works:
                works.map(savefile, args)

