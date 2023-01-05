from time import time
import socket

HOST = "127.0.0.1"
PORT = 65444
CHUNK_SIZE = 2048

start = time()

def reduce(map_):
    pass

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
    data = s.recv(1024)
    data = s.recv(1024)
    data = s.recv(1024)


