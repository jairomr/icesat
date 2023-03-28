from time import sleep
#from rich import print
import subprocess
from icesat2.config import settings



subprocess.Popen(["ssh", "-p", "2522", "-fN", f"root@{settings.SERVER}", "-L" f"{settings.DB_PORT}:127.0.0.1:{settings.DB_PORT}"])
while True:
    print('on')
    sleep(3000)