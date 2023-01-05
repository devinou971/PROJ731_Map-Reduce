from time import time, sleep
import socket

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 65444  # The port used by the server
CHUNK_SIZE = 16384

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
    result = {}
    for word in content.split(" "):
        # reducer_id = len(item[0]) % nb_reducers
        if word in result:
            result[word] += 1
        else : 
            result[word] = 1
    
    return result



with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    ok = False
    while not ok:
        try:
            s.connect((HOST, PORT))
        except:
            PORT += 1
            ok = True
    print("Sever port:", PORT)

    # ==================== GET MAPPER ID ==================== 
    s.sendall(b"mapper")
    d1 = s.recv(1024)
    id = int.from_bytes(d1, 'little', signed=False)

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
    while len(content.split("\n")) < text_length:
        d = s.recv(CHUNK_SIZE)
        content += d.decode("utf-8")
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
    string = ""
    for item in m.items():
        string += f"{item[0]} {item[1]}\n"
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