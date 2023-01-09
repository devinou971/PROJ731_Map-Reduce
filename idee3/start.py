import os
from time import sleep
from dotenv import load_dotenv
import os 

load_dotenv()
NB_REDUCERS = int(os.getenv('NB_REDUCERS'))
NB_MAPPERS = int(os.getenv('NB_MAPPERS'))

os.system("python3 main.py&")
sleep(0.1)

for i in range(NB_MAPPERS):
    os.system("python3 mapper.py&")

sleep(0.1)
for i in range(NB_REDUCERS):
    os.system("python3 reducer.py&")

