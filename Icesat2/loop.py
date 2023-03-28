from time import sleep
#from rich import print
import subprocess
from icesat2.config import settings



subprocess.Popen(["ssh", "-p", "2522", "-fN", f"root@{settings.SERVER}", "-L" "27018:127.0.0.1:27017"])
while True:
    print('on')
    sleep(3000)