from requests import Session
import tempfile

from rich import print
import geopandas as gpd
from icesat2.config import settings, logger
from icesat2.utils import process_atl08
import pandas as pd
from multiprocessing import Pool
#from sqlalchemy import create_engine
#
#engine = create_engine((
#  f"postgresql://{settings.DB_USER}:"
#  f"{settings.DB_PASS}@{settings.DB_HOST}"
#  f":{settings.DB_PORT}/{settings.DATABASE}"
#
#)
#)  
from pymongo import MongoClient,GEOSPHERE
import shapely.geometry
 
myclient = MongoClient(f"mongodb://{settings.DB_HOST}:{settings.DB_PORT}/")

# database
db = myclient["icesat2"]
 
# Created or Switched to collection
# names: GeeksForGeeks
collection = db["icesat2v3"]
collection.create_index([("geometry", GEOSPHERE)])
 




def savefile(url):
  namefile = url.split('/')[-1] 
  try:
    logger.info("Tentado baixar: "+namefile)
    logger.info("Logandd: "+settings.username)
    with Session() as session:
      session.auth = (settings.username, settings.password)
      r1 = session.request('get', url)
      r = session.get(r1.url, auth=(settings.username, settings.password))
      if r.ok:
        logger.info("Ok Baixando "+namefile) # Say
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_name = f'{tmpdirname}/{namefile}'
            with open(file_name, 'wb') as f:
                f.write(r.content)
            df = process_atl08(file_name)
            df['file'] = namefile
            gdf = gpd.GeoDataFrame(df,crs=4326, geometry=gpd.points_from_xy(df['longitude'], df['latitude']))
            gdf = gdf.drop(columns=['longitude','latitude'])
            gdf['geometry']=gdf['geometry'].apply(lambda x:shapely.geometry.mapping(x))
            data = gdf.to_dict(orient='records')
            collection.insert_many(data)
            logger.info(f'Save {file} in db') 
            # Processo da Hunter    
        
  except Exception as e:
    logger.exception(str(e))
    
if __name__ == '__main__':
  files_runs = collection.distinct("file")
  df=pd.read_csv('urls.dat')
  df['file'] = df['url'].apply(lambda x: x.split('/')[-1])
  total = len(df)
  df = df[~df['file'].isin(files_runs)]
  complet = len(df)
  logger.info(f'Feito {total-complet} de {total} falta {complet}')

  url = 'https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL08/005/2022/10/12/ATL08_20221012220720_03391701_005_01.h5'
  file = url.split('/')
  with Pool(settings.CORE) as works:
    works.map(savefile,df['url'])
    
    #savefile()