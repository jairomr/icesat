from requests import Session
import tempfile

from rich import print
import geopandas as gpd
from icesat2.config import settings, logger
from icesat2.utils import process_atl08
import pandas as pd
from multiprocessing import Pool
from sqlalchemy import create_engine

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


def savefile(args):
    url, session = args
    namefile = url.split('/')[-1]
    try:
        logger.info('Tentado baixar: ' + namefile)
        r1 = session.request('get', url)
        r = session.get(r1.url, auth=(settings.username, settings.password))
        if r.ok:
            logger.info('Ok Baixando ' + namefile)   # Say
            with tempfile.TemporaryDirectory() as tmpdirname:
                file_name = f'{tmpdirname}/{namefile}'
                with open(file_name, 'wb') as f:
                    f.write(r.content)
                df = process_atl08(file_name)
                if len(df) > 0:
                    gdf = gpd.GeoDataFrame(
                        df,
                        crs=4326,
                        geometry=gpd.points_from_xy(
                            df['longitude'], df['latitude']
                        ),
                    )
                    gdf = gdf.drop(columns=['longitude', 'latitude'])

                    gdf.to_postgis(
                        'atl08_raw', engine, if_exists='append', index=False
                    )

                    with MongoClient(
                        f'mongodb://{settings.DB_HOST}:{settings.DB_PORT_MONGO}/'
                    ) as client:

                        db = client['icesat2']
                        collection = db['icesat2v3']
                        collection.insert_one(
                            {
                                'file': namefile,
                                'url': url,
                                'status': 'downloaded',
                            }
                        )
                    logger.success(f'Foi salvo o {file_name}')
                else:
                    with MongoClient(
                        f'mongodb://{settings.DB_HOST}:{settings.DB_PORT_MONGO}/'
                    ) as client:
                        db = client['icesat2']
                        collection = db['icesat2v3']
                        collection.insert_one(
                            {
                                'file': namefile,
                                'url': url,
                                'status': 'empty file',
                            }
                        )
                        logger.warning(f'Esta vazio {file_name}')

    except Exception as e:
        logger.exception('Error not mapeado')
        with MongoClient(
            f'mongodb://{settings.DB_HOST}:{settings.DB_PORT_MONGO}/'
        ) as client:
            db = client['icesat2']
            collection = db['icesat2v3']
            collection.insert_one(
                {'file': namefile, 'url': url, 'status': str(e)}
            )


if __name__ == '__main__':
    with MongoClient(
        f'mongodb://{settings.DB_HOST}:{settings.DB_PORT_MONGO}/'
    ) as client:
        db = client['icesat2']
        collection = db['icesat2v3']

        files_runs = collection.find(
            {'$or': [{'status': 'downloaded'}, {'status': 'empty file'}]}
        ).distinct('file')

    df = pd.read_csv('urls.dat')
    df['file'] = df['url'].apply(lambda x: x.split('/')[-1])
    total = len(df)
    df = df[~df['file'].isin(files_runs)]
    complet = len(df)
    logger.info(f'Feito {total-complet} de {total} falta {complet}')

    with Session() as session:
        session.auth = (settings.username, settings.password)
        logger.info('Logandd: ' + settings.username)
        # savefile((df['url'].iloc[0],session))

        args = [(url, session) for url in df['url']]
        with Pool(settings.CORE) as works:
            works.map(savefile, args)
