from node_config import NODES
import socket

class Bully:
    def __init__(self, pid):
        self.pid = pid

    def run_election(self):
        maior = self.pid
        for p in NODES:
            if p > self.pid:
                try:
                    sock = socket.socket()
                    sock.settimeout(1)
                    sock.connect(NODES[p])
                    sock.sendall(b"ARE_YOU_ALIVE")
                    resp = sock.recv(16)
                    sock.close()
                    if resp == b"OK":
                        maior = p
                except Exception:
                    continue
        return maior
