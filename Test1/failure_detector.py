from node_config import NODES
import socket
import threading
import time

class FailureDetector:
    def __init__(self, pid, callback_on_failure):
        self.pid = pid
        self.callback = callback_on_failure
        self.alive = {}
        self.lock = threading.Lock()

    def start(self):
        threading.Thread(target=self._monitor, daemon=True).start()

    def _monitor(self):
        while True:
            for p in NODES:
                if p == self.pid:
                    continue
                try:
                    sock = socket.socket()
                    sock.settimeout(1)
                    sock.connect(NODES[p])
                    sock.sendall(b"PING")
                    resp = sock.recv(16)
                    sock.close()
                    if resp == b"OK":
                        with self.lock:
                            self.alive[p] = True
                    else:
                        raise Exception("Sem resposta OK")
                except:
                    with self.lock:
                        if self.alive.get(p, True):
                            self.alive[p] = False
                            self.callback(p)
            time.sleep(5)

    def get_alive_peers(self):
        with self.lock:
            return [p for p in self.alive if self.alive[p]]
