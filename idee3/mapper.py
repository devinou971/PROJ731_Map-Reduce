from time import time
import socket

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 65444  # The port used by the server
CHUNK_SIZE = 2048

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
    result = [{} for i in range(nb_reducers)]
    for word in content.split(" "):
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
        except:
            PORT += 1
            ok = True
    
    print(PORT)
    s.sendall(b"mapper")
    d1 = s.recv(1024)
    id = int.from_bytes(d1, 'little', signed=False)

    s.sendall(b"nbreducers")
    d2 = s.recv(1024)
    nb_reducers = int.from_bytes(d2, 'little', signed=False)
    
    print(f"Id {id} ({d1}), nb Reducers : {nb_reducers} ({d2})")

    s.sendall(b"text_length")
    d3  = s.recv(CHUNK_SIZE)
    text_length_str = d3.decode("utf-8")
    text_length = int(text_length_str)
    print(f"Text length : {text_length} bytes")

    # ==================== GETTING DATA ====================
    print("Downloading text ... ", end="")
    s.sendall(b"text")
    content = ""
    for i in range(0, text_length, CHUNK_SIZE):
        d = s.recv(CHUNK_SIZE)
        content += d.decode("utf-8")
    print("Done")

    # ==================== MAPPING ====================
    print("Mapping ... ", end="")
    m = map(content, nb_reducers)
    print("Done")
    s.sendall(b"finished")

    # ==================== WAITING FOR THE GO ====================
    data = s.recv(1024)
    print(data)

    # ==================== SENDING DATA BACK ====================

    s.sendall(b"sending_map")
    for item in m.items():
        string = f"{item[0]} {item[1]}"
        s.sendall(bytes(string, encoding="utf-8"))
    s.sendall(b"finished")
    

    # Just used to pause the thing.
    


    data = s.recv(1024)
    print(data)
    end = time()
    print("Time taken :", end - start)