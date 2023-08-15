from icesat2.config import logger, settings
from icesat2.model.atl import Base
from sqlalchemy import create_engine

engine = create_engine(
    (
        f'postgresql://{settings.DB_USER}:'
        f'{settings.DB_PASS}@{settings.DB_HOST}'
        f':{settings.DB_PORT}/{settings.DATABASE}'
    )
)
