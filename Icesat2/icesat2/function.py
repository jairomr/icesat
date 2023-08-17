import geohash
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from icesat2.config import logger, settings



def to_geohash(row):
    try:
        return geohash.encode(
            latitude=row['latitude'], longitude=row['longitude'], precision=3
        )
    except KeyError:
        return geohash.encode(
            latitude=row['lat_ph'], longitude=row['lon_ph'], precision=3
        )
    


def geohash_lapig(tmp_df):
    tmp_df['geohash'] = tmp_df.apply(to_geohash, axis=1)
    filtered_df = tmp_df[
        tmp_df['geohash'].str.startswith(('d', '6', '7'))
    ].copy()
    return filtered_df


def atl82atl3(name):
    return name.replace('ATL08', 'ATL03')





def saveMongo(doc,successo='Salvo com sucesso',duplicado='Salvo com sucesso'):
    _id = doc['_id']
    with MongoClient(
        f'mongodb://{settings.MONGO_HOST}:{settings.DB_PORT_MONGO}/') as client:
        db = client['icesat2']
        collection = db[settings.COLLECTION_NAME]
        try:
            collection.insert_one(doc)
            logger.warning(f'{_id}:{successo}')
        except DuplicateKeyError:
            collection.update_one({'_id': _id}, {'$set': doc})
            logger.warning(f'{_id}: {duplicado}')