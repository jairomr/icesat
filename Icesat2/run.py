from requests import Session
import tempfile

from rich import print
import geopandas as gpd
from icesat2.config import settings, logger
from icesat2.utils import process_atl08
from sqlalchemy import create_engine

engine = create_engine((
  f"postgresql://{settings.DB_USER}:"
  f"{settings.DB_PASS}@{settings.DB_HOST}"
  f":{settings.DB_PORT}/{settings.DATABASE}"

)
)  

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
            print(gdf)
            # Processo da Hunter    
        
  except Exception as e:
    logger.exception(str(e))
    
if __name__ == '__main__':
  savefile('https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/ATLAS/ATL08/005/2022/10/12/ATL08_20221012220720_03391701_005_01.h5')