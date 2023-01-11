from time import time
from socket import socket, AF_INET, SOCK_STREAM
import threading 
from json import loads, dumps
from dotenv import load_dotenv
from os import listdir, getenv, remove

load_dotenv()
HOST = getenv('HOST')
PORT = int(getenv('PORT'))
NB_REDUCERS = int(getenv('NB_REDUCERS'))
NB_MAPPERS = int(getenv('NB_MAPPERS'))
CHUNK_SIZE = int(getenv('CHUNK_SIZE'))

current_mapper_id = 0
current_reducer_id = 0
ready = [False for _ in range(NB_MAPPERS)]
reducing_finished = [False for _ in range(NB_REDUCERS)]

threads = []
files = ["../data/mon_combat_utf8.txt"]
file_contents = []
for file in files:
    # ISO-8859-1
    with open(file, "r", encoding="utf8") as f:
    # with open(file, "r", encoding="ISO-8859-1") as f:
        file_contents += f.readlines()

class MapperThread(threading.Thread):
    def __init__(self, ip, port, clientsocket, id, file_contents) -> None:
        threading.Thread.__init__(self)
        self.ip = ip 
        self.port = port
        self.clientsocket = clientsocket
        
        self.type = "Mapper"
        self.id = id
        self.file_contents = file_contents
        self.nb_lines = len(file_contents)

        print("[+] New thread for %s %s" % (self.ip, self.port))

    def run(self): 
        global current_mapper_id, current_reducer_id, ready
        print("Connection from %s %s" % (self.ip, self.port))

        while True:
            r = self.clientsocket.recv(1024)
            
            if r == b"nbreducers":
                self.clientsocket.sendall(NB_REDUCERS.to_bytes(2, 'little', signed=False))
            
            elif r == b"nbmappers":
                self.clientsocket.sendall(NB_MAPPERS.to_bytes(2, 'little', signed=False))

            elif r == b"text_length":
                # We send the length of the texte first in bytes
                print(f"Evaluating text size for mapper {self.id} ...")
                text = " ".join(self.file_contents[int(self.nb_lines / NB_REDUCERS * self.id) : int(self.nb_lines / NB_REDUCERS * (self.id+1))])
                text_to_send = bytes(text, encoding="utf8")
                length = len(text.split("\n"))
                length_str = bytes(str(length), encoding="utf8")
                self.clientsocket.sendall(length_str)

            elif r == b"text":

                # Then  we send all the text
                text_to_send = " ".join(self.file_contents[int(self.nb_lines / NB_REDUCERS * self.id) : int(self.nb_lines / NB_REDUCERS * (self.id+1))])
                text_to_send = bytes(text_to_send, encoding="utf8")
                print(f"Sending text to mapper {self.id} ... ", end="")
                self.clientsocket.sendall(text_to_send)
                self.clientsocket.recv(1024)
                print("Done")

                self.clientsocket.sendall(b"go")
            
            elif r == b"map_size":
                print("Receiving the map")
                length = int.from_bytes(self.clientsocket.recv(1024), "big")
                print("Length of data :", length)
                received_text = ""
                received_bytes = b""
                
                # We are ready to receive the map
                answer = f"ok{self.id}" 
                self.clientsocket.sendall(bytes(answer, encoding="utf-8"))

                while len(received_bytes.split(b"\n")) < length:
                    received_bytes += self.clientsocket.recv(CHUNK_SIZE)
                received_text = received_bytes.decode("utf-8")

                print(f"Mapper {self.id} has finished sending raw data")
                self.clientsocket.sendall(bytes(answer, encoding="utf-8"))
                
                maps = loads(received_text)
                for i in range(NB_REDUCERS) : 
                    with open(f"outputs/m{self.id}_r{i}.json", "w", encoding="utf-8") as f:
                        m = maps[i]
                        text = dumps(m).replace(", \"", ", \n \"")
                        f.write(text)

                ready[self.id] = True
                print(ready)

            if not r : 
                break

        print(self.type, self.id, "disconnected ...")

class ReducerThread(threading.Thread):
    def __init__(self, ip, port, clientsocket, id, file_contents) -> None:
        threading.Thread.__init__(self)
        self.ip = ip 
        self.port = port
        self.clientsocket = clientsocket
        
        self.type = "Reducer"
        self.id = id
        self.file_contents = file_contents
        self.nb_lines = len(file_contents)

        print("[+] New thread for %s %s" % (self.ip, self.port))


    def run(self): 
        global ready
        print("Connection from %s %s" % (self.ip, self.port))

        while ready != [True for _ in range(NB_REDUCERS)]:
            pass
        self.setup_reducer()

        print(self.type, self.id, "disconnected ...")
    
    def setup_reducer(self):
        global reducing_finished
        map_files = listdir("outputs")
        all_maps = "["
        for map_file in map_files:
            if map_file.endswith(f"_r{self.id}.json"):
                with open("outputs/"+map_file, "r", encoding="utf8") as f:
                    all_maps += f.read() + ",\n" 
        
        all_maps  = all_maps[:-2] + "]"

        print(f"Sending map size to reducer {self.id}")
        length = len(all_maps.split("\n"))
        length_str = str(length)
        self.string = all_maps
        self.clientsocket.sendall(bytes(length_str, encoding="utf-8"))

        # Wait for a client response before continuing
        self.clientsocket.recv(1024)

        # Sending the full maps
        self.clientsocket.sendall(bytes(all_maps, encoding="utf-8"))

        # We then need to receive the reduced map : 
        length_str = self.clientsocket.recv(1024).decode("utf-8")
        length = int(length_str)

        self.clientsocket.sendall(b"ok")

        received_text = ""
        received_bytes = b""

        while len(received_bytes.split(b"\n")) < length:
            received_bytes += self.clientsocket.recv(CHUNK_SIZE)
        received_text = received_bytes.decode("utf-8")

        with open(f"outputs/r{self.id}.json", "w", encoding="utf-8") as f :
            f.write(received_text)

        self.clientsocket.sendall(bytes("received", encoding="utf-8"))

        # Wait for the reducer to return the ok
        self.clientsocket.recv(1024)

        self.clientsocket.close()
        reducing_finished[self.id] = True

# Purging the output folder 
for i in listdir("outputs"):
    remove("outputs/"+i)

start = time()

# Starting the main manager process
with socket(AF_INET, SOCK_STREAM) as s : 
    ok = False
    while not ok:
        try:
            s.bind((HOST, PORT))
            ok = True
        except:
            PORT += 1
    print("Sever starting at port", PORT)
    s.listen(100)
    while current_mapper_id < NB_MAPPERS - 1 or current_reducer_id < NB_REDUCERS :
        (clientsocket, (ip, port)) = s.accept()
        thread_type = clientsocket.recv(1024)
        if thread_type == b"mapper":
            print(current_mapper_id < NB_MAPPERS)
            if current_mapper_id < NB_MAPPERS:
                clientsocket.sendall(current_mapper_id.to_bytes(2, 'little', signed=True))
                newthread = MapperThread(ip, port, clientsocket, current_mapper_id, file_contents)
                current_mapper_id += 1
                newthread.start()
            else : 
                not_allowed = -1
                clientsocket.sendall(not_allowed.to_bytes(2, 'little', signed=True))
        
        elif thread_type == b"reducer":
            if current_reducer_id < NB_MAPPERS:
                clientsocket.sendall(current_reducer_id.to_bytes(2, 'little', signed=True))
                newthread = ReducerThread(ip, port, clientsocket, current_reducer_id, file_contents)
                current_reducer_id += 1
                newthread.start()
            else: 
                not_allowed = -1
                clientsocket.sendall(not_allowed.to_bytes(2, 'little', signed=True))
        
        threads.append(newthread)

all_true = [True for _ in range(NB_REDUCERS)]
while reducing_finished != all_true:
    pass
final_file_content = ""
for filename in listdir("outputs"):
    if filename.startswith("r") and filename.endswith(".json"):
        with open("outputs/"+filename, "r", encoding="utf-8") as f:
            final_file_content += f.read()[1:-1] + ","

final_file_content = "{" + final_file_content[:-1] + "}" 

with open("outputs/wordlist.json", "w", encoding="utf-8") as f:
    f.write(final_file_content)
print("Manager finished")