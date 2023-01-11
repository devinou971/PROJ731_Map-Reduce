from time import time
import socket
import json
from dotenv import load_dotenv
import os 

load_dotenv()
HOST = os.getenv('HOST')
PORT = int(os.getenv('PORT'))
NB_REDUCERS = int(os.getenv('NB_REDUCERS'))
NB_MAPPERS = int(os.getenv('NB_MAPPERS'))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE'))

start = time()

def map(content, nb_reducers):
    content = content.replace(",", " ")
    content = content.replace(".", " ")
    content = content.replace("\\n"," ")
    content = content.replace("\n"," ")
    content = content.replace("\t"," ")
    content = content.replace("\\t"," ")
    content = content.replace("!"," ")
    content = content.lower()
    result = [{} for _ in range(nb_reducers)]
    for word in content.split(" "):
        if(len(word) > 0):
            reducer_id = len(word) % nb_reducers
            if word in result[reducer_id]:
                result[reducer_id][word] += 1
            else : 
                result[reducer_id][word] = 1
    
    return result

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    ok = False
    while not ok:
        try:
            s.connect((HOST, PORT))
            ok = True

        except:
            PORT += 1
    print("Sever port:", PORT)

    # ==================== GET MAPPER ID ==================== 
    s.sendall(b"mapper")
    d1 = s.recv(1024)
    id = int.from_bytes(d1, 'little', signed=True)
    if id == -1:
        print("Mapping not needed")
        s.close()
        exit(0)

    # ==================== GET NUMBER OF REDUCERS ====================
    s.sendall(b"nbreducers")
    d2 = s.recv(1024)
    nb_reducers = int.from_bytes(d2, 'little', signed=False)
    
    print(f"Id {id}, nb Reducers : {nb_reducers}")

    # ==================== GET DATA LENGTH ====================
    s.sendall(b"text_length")
    d3  = s.recv(1024)
    text_length_str = d3.decode("utf-8")
    text_length = int(text_length_str)
    print(f"Text length : {text_length} lines")

    # ==================== GETTING DATA ====================
    print("Downloading text ... ", end="")
    s.sendall(b"text")
    i = 0
    content = ""
    byte_content = b""
    while len(byte_content.split(b"\n")) < text_length:
        byte_content += s.recv(CHUNK_SIZE)
        # print(len(content.split("\n")), text_length)
    content = byte_content.decode("utf-8")
    print("Done")

    # ==================== MAPPING ====================
    print("Mapping ... ", end="")
    m = map(content, nb_reducers)
    print("Done")
    s.sendall(b"finished")

    # ==================== WAITING FOR THE GO SIGNAL ====================
    s.recv(1024)

    # ==================== SENDING DATA BACK ====================
    s.sendall(b"map_size")
    string = json.dumps(m)
    string = string.replace(", \"", ",\n \"")
    string_bytes = string.encode("utf-8")

    length = len(string.split("\n"))
    length_bytes = length.to_bytes(4, "big")
    print("Send the size of the map", length)
    s.sendall(length_bytes)
    print("Sent Map Size")
    # Waiting for the "ok" instruction 
    data = s.recv(1024)
    print("Sending back the map ...", end="")
    s.sendall(string_bytes)
    data = s.recv(1024)
    print("Done")
    
    end = time()
    print("Time taken :", end - start)