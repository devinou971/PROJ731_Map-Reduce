from time import time
import socket
import threading 
start = time()

HOST = "127.0.0.1"
PORT = 65444
NB_REDUCERS = 4
NB_MAPPERS = 4
CHUNK_SIZE = 16384

current_mapper_id = 0
current_reducer_id = 0
nb_reducers = 0
ready = [False for _ in range(NB_MAPPERS)]
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
        global current_mapper_id, current_reducer_id, ready, nb_reducers
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
                nb_reducers += 1

            elif r == b"nbreducers":
                self.clientsocket.sendall(NB_REDUCERS.to_bytes(2, 'little', signed=False))
            
            elif r == b"nbmappers":
                self.clientsocket.sendall(NB_MAPPERS.to_bytes(2, 'little', signed=False))

            elif r == b"text_length":
                # We send the length of the texte first in bytes
                print(f"Evaluating text size for mapper {self.id} ...")
                text = " ".join(file_contents[int(nb_lines / NB_REDUCERS * self.id) : int(nb_lines / NB_REDUCERS * (self.id+1))])
                text_to_send = bytes(text, encoding="utf8")
                length = len(text.split("\n"))
                length_str = bytes(str(length), encoding="utf8")
                self.clientsocket.sendall(length_str)

            elif r == b"text":

                # Then  we send all the text
                text_to_send = " ".join(file_contents[int(nb_lines / NB_REDUCERS * self.id) : int(nb_lines / NB_REDUCERS * (self.id+1))])
                text_to_send = bytes(text_to_send, encoding="utf8")
                print(f"Sending text to mapper {self.id} ... ", end="")
                self.clientsocket.sendall(text_to_send)
                print("Done")
                self.clientsocket.recv(1024)
                print("response received")
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

                with open(f"{self.id}.txt", "w") as f:
                    f.write(received_text)
                
                print(f"Mapper {self.id} has finished sending raw data")
                self.clientsocket.sendall(bytes(answer, encoding="utf-8"))
                
                maps = [{} for _ in range(NB_REDUCERS)]

                for line in received_text.split("\n"):
                    item = line.split(" ")
                    
                    reducer_id = len(item[0]) % NB_REDUCERS
                    try:
                        maps[reducer_id][item[0]] = int(item[1])
                    except Exception as e:
                        pass

                ready[self.id] = True
                print(ready)

            if not r : 
                break

        print(self.type, self.id, "disconnected ...")

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
        
    # end = time()
    # print("Time taken :", end - start)
        