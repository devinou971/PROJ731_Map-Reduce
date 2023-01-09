from time import time
import socket
import json
from dotenv import load_dotenv
import os 
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

load_dotenv()
HOST = os.getenv('HOST')
PORT = int(os.getenv('PORT'))
NB_REDUCERS = int(os.getenv('NB_REDUCERS'))
NB_MAPPERS = int(os.getenv('NB_MAPPERS'))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE'))

start = time()

def reduce(maps):
    result = {}
    for c in maps:
        for word in c.keys():
            if word in result:
                result[word] += c[word]
            else:
                result[word] = c[word]
    return result

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    ok = False
    while not ok:
        try:
            s.connect((HOST, PORT))
        except:
            PORT += 1
            ok = True
    
    print(f"PORT: {PORT}")

    # ==================== SENDING IDENTITY ====================
    s.sendall(b"reducer")
    d1 = s.recv(1024)
    id = int.from_bytes(d1, 'little', signed=True)
    if id == -1:
        print("Reducer not needed")
        s.close()
        exit(0)
    print("Reducer with Id :", id)

    # ==================== GET MAP SIZE ====================

    d3  = s.recv(1024)
    text_length_str = d3.decode("utf-8")
    text_length = int(text_length_str)
    s.sendall(b"ok")
    print(f"Text length : {text_length} lines")

    # ==================== GETTING MAPS ====================
    print("Downloading text ... ", end="")
    content = ""
    while len(content.split("\n")) < text_length:
        d = s.recv(CHUNK_SIZE)
        content += d.decode("utf-8")
    maps = json.loads(content)
    print("Done")

    # ==================== REDUCE THE MAPS ====================
    print("Reducing maps ... ", end="")
    reduced_map = reduce(maps)

    with open(f"outputs/r{id}.json", "w", encoding="utf-8") as f :
        f.write(json.dumps(reduced_map))
    print("Done")


    # ==================== INFORM SERVER OF SUCCESS ====================
    s.sendall(b"done")

    end = time()
    print("Time taken :", end - start)