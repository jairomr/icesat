# from rich import print
import subprocess
from time import sleep

from icesat2.config import settings

subprocess.Popen(
    [
        'ssh',
        '-p',
        '2522',
        '-fN',
        f'root@{settings.SERVER}',
        '-L' f'{settings.DB_PORT}:127.0.0.1:{settings.DB_PORT}',
    ]
)
subprocess.Popen(
    [
        'ssh',
        '-p',
        '2522',
        '-fN',
        f'root@{settings.SERVER}',
        '-L' f'{settings.DB_PORT_MONGO}:127.0.0.1:{settings.DB_PORT_MONGO}',
    ]
)
while True:
    print('on')
    sleep(30)
