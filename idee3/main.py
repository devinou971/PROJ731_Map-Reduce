import socket
import threading 
import sys

HOST = "127.0.0.1"
PORT = 65444
NB_REDUCERS = 4
NB_MAPPERS = 4

current_mapper_id = 0
current_reducer_id = 0
ready = False
stop_threads = False

threads = []
files = ["../data/bible.txt"]
file_contents = []

class ClientThread(threading.Thread):
    def __init__(self, ip, port, clientsocket) -> None:
        threading.Thread.__init__(self)
        self.ip = ip 
        self.port = port
        self.clientsocket = clientsocket
        
        self.type = None
        self.id = None

        print("[+] New thread for %s %s" % (self.ip, self.port))

    def run(self): 
        global current_mapper_id, current_reducer_id, ready
        print("Connection from %s %s" % (self.ip, self.port))

        while True:
            r = self.clientsocket.recv(1024)
            if r == b"mapper":
                self.clientsocket.sendall(current_mapper_id.to_bytes(2, 'little', signed=False))
                self.type = "mapper"
                self.id = current_mapper_id 
                current_mapper_id += 1

            elif r == b"reducer":
                self.clientsocket.sendall(current_reducer_id.to_bytes(2, 'little', signed=False))
                self.type = "reducer"
                self.id = current_reducer_id 
                current_reducer_id += 1
                ready = current_reducer_id == NB_REDUCERS - 1

            elif r == b"nbreducers":
                self.clientsocket.sendall(NB_REDUCERS.to_bytes(2, 'little', signed=False))
            
            elif r == b"nbmappers":
                self.clientsocket.sendall(NB_MAPPERS.to_bytes(2, 'little', signed=False))

            elif r == b"text_length":
                # We send the length of the texte first in bytes
                print(f"Evaluating text size for mapper {self.id} ...")
                text_to_send = " ".join(file_contents[int(nb_lines / NB_REDUCERS * self.id) : int(nb_lines / NB_REDUCERS * (self.id+1))])
                text_to_send = bytes(text_to_send, encoding="utf8")
                length = sys.getsizeof(text_to_send)
                length_str = bytes(str(length), encoding="utf8")
                self.clientsocket.sendall(length_str)

            elif r == b"text":
                # Then  we send all the text
                text_to_send = " ".join(file_contents[int(nb_lines / NB_REDUCERS * self.id) : int(nb_lines / NB_REDUCERS * (self.id+1))])
                text_to_send = bytes(text_to_send, encoding="utf8")
                print(f"Sending text to mapper {self.id} ... ", end="")
                self.clientsocket.sendall(text_to_send)
                print("Done")

            elif r == b"finished":
                print("Here")
                reducers_present = False
                while not reducers_present : 
                    nb_reducers = 0
                    for thread in threads:
                        if thread.type == "reducer":
                            nb_reducers += 1
                    #print(nb_reducers)
                    if nb_reducers == NB_REDUCERS:
                        self.clientsocket.sendall(b"go")
                        reducers_present = True
            
            elif r == b"sending_map":
                map_ = {}
                while self.clientsocket.recv(1024) != b"finished":
                    item = self.clientsocket.recv(1024)
                    data = item.decode("utf-8").split(" ")
                    map_[data[0]] = int(data[1])
                
                print(map_)

            if not r : 
                break

        print("Client déconnecté...")

for file in files:
    with open(file, "r", encoding="utf8") as f:
        file_contents += f.readlines()

nb_lines = len(file_contents)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s : 
    ok = False
    while not ok:
        try:
            s.bind((HOST, PORT))
            
        except:
            PORT += 1
            ok = True
    print(PORT)
    s.listen(10)
    while(True) :
        (clientsocket, (ip, port)) = s.accept()
        newthread = ClientThread(ip, port, clientsocket)
        newthread.start()
        
        threads.append(newthread)
        