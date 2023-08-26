import pickle
import geohash
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from icesat2.config import logger, settings

with open('geohash_5_maior_20.sort.obj','rb') as f:
    filter_geohash = pickle.load(f)

def point_is_patagem(item, esquerda=0, direita=122850 ):
    """Implementa pesquisa binária recursivamente."""
    # 1. Caso base: o elemento não está presente. 
    if direita < esquerda:
            return False
    meio = (esquerda + direita) // 2
    # 2. Nosso palpite estava certo: o elemento está no meio do arranjo. 
    if filter_geohash[meio] == item:
            return True
        # 3. O palpite estava errado: atualizamos os limites e continuamos a busca. 
    elif filter_geohash[meio] > item:
        return point_is_patagem( item, esquerda, meio - 1 )
    else: # A[meio] < item
        return point_is_patagem( item, meio + 1, direita)



def to_geohash(row):
    try:
        return geohash.encode(
            latitude=row['latitude'], longitude=row['longitude'], precision=5
        )
    except KeyError:
        return geohash.encode(
            latitude=row['lat_ph'], longitude=row['lon_ph'], precision=5
        )





def geohash_lapig(tmp_df):
    # '6v7','6vk','6v5','6vh'
    tmp_df['geohash'] = tmp_df.apply(to_geohash, axis=1)
    filtered_df = tmp_df[
        tmp_df['geohash'].apply(point_is_patagem)
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