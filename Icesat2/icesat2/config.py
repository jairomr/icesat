from dynaconf import Dynaconf
from loguru import logger

# default
settings = Dynaconf(settings_files=["settings.toml"])

# using root_path
settings = Dynaconf(

    settings_files=["settings.toml", "*.yaml"],
)

logger.add("file_1.log", rotation="500 MB") 