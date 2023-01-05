from time import time
import socket
import threading 
import json
from dotenv import load_dotenv
import os 
from os import listdir
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)
# from MapperThread import MapperThread
# from ReducerThread import ReducerThread

load_dotenv()
HOST = os.getenv('HOST')
PORT = int(os.getenv('PORT'))
NB_REDUCERS = int(os.getenv('NB_REDUCERS'))
NB_MAPPERS = int(os.getenv('NB_MAPPERS'))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE'))

start = time()

current_mapper_id = 0
current_reducer_id = 0
ready = [False for _ in range(NB_MAPPERS)]

threads = []
files = ["../data/bible.txt"]
file_contents = []
for file in files:
    with open(file, "r", encoding="utf8") as f:
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
                
                # We are ready to receive the map
                answer = f"ok{self.id}" 
                self.clientsocket.sendall(bytes(answer, encoding="utf-8"))

                while len(received_text.split("\n")) < length:
                    d = self.clientsocket.recv(CHUNK_SIZE)
                    received_text += d.decode("utf-8")
                
                print(f"Mapper {self.id} has finished sending raw data")
                self.clientsocket.sendall(bytes(answer, encoding="utf-8"))
                
                # received_text = received_text.replace(", \"", ", \n \"")
                maps = json.loads(received_text)
                for i in range(NB_REDUCERS) : 
                    with open(f"outputs/m{self.id}_r{i}.json", "w", encoding="utf-8") as f:
                        m = maps[i]
                        text = json.dumps(m).replace(", \"", ", \n \"")
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
        global current_mapper_id, current_reducer_id, ready
        print("Connection from %s %s" % (self.ip, self.port))

        while ready != [True for _ in range(NB_REDUCERS)]:
            pass
        self.setup_reducer()

        print(self.type, self.id, "disconnected ...")
    
    def setup_reducer(self):
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

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s : 
    ok = False
    while not ok:
        try:
            s.bind((HOST, PORT))
        except:
            PORT += 1
            ok = True
    print("Sever starting at port", PORT)
    s.listen(10)
    while(True) :
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
        
    # end = time()
    # print("Time taken :", end - start)
        