import socket
import json
import threading
import time
from node_config import NODES, NODES_LOCK

class Paxos:
    def __init__(self, pid, retries=3, timeout=2):
        self.pid = pid
        self.retries = retries
        self.timeout = timeout
        self.lock = threading.Lock()

    def get_other_nodes(self):
        """ Retorna uma cópia segura dos outros nós na rede. """
        with NODES_LOCK:
            # Retorna todos os nós, exceto o próprio
            return {p: addr for p, addr in NODES.items() if p != self.pid}

    def send(self, address, message):
        """ Tenta enviar uma mensagem para um endereço com retentativas. """
        for attempt in range(1, self.retries + 1):
            try:
                # Cria um novo socket para cada tentativa
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect(address)
                sock.sendall(json.dumps(message).encode())
                response_data = sock.recv(2048)
                sock.close()
                if response_data:
                    return json.loads(response_data.decode())
                return None # Resposta vazia
            except (socket.timeout, ConnectionRefusedError, ConnectionResetError) as e:
                # Erros esperados de rede
                print(f"[PAXOS {self.pid}] Tentativa {attempt} para {address} falhou: {e}")
                time.sleep(0.5) # Espera antes de tentar novamente
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"[PAXOS {self.pid}] Erro ao decodificar resposta de {address}: {e}")
                return None # Resposta mal formatada
            except Exception as e:
                print(f"[PAXOS {self.pid}] Erro inesperado ao comunicar com {address}: {e}")
                return None
        print(f"[PAXOS {self.pid}] Falha final ao comunicar com {address} após {self.retries} tentativas.")
        return None

    def propose(self, rodada, valor_inicial):
        with self.lock:
            nodes_to_contact = self.get_other_nodes()
            
            with NODES_LOCK:
                total_nodes = len(NODES)
            
            quorum = (total_nodes // 2) + 1
            
            # CORREÇÃO: O proposal_id usa o formato 'rodada-pid'
            # A lógica de comparação foi corrigida no acceptor (socket_server em main.py)
            proposal_id = f"{rodada}-{self.pid}"

            print(f"[PAXOS {self.pid}] Iniciando proposta '{proposal_id}' para {len(nodes_to_contact)} nós com valor inicial {valor_inicial:.4f}. Quorum necessário: {quorum}")

            # Fase 1: Prepare
            promises = []
            accepted_values_from_peers = []
            prepare_msg = {"type": "prepare", "proposal_id": proposal_id}

            for p, addr in nodes_to_contact.items():
                response = self.send(addr, prepare_msg)
                if response and response.get("ok"):
                    promises.append(p)
                    if response.get("accepted_id") and response.get("accepted_value") is not None:
                        accepted_values_from_peers.append(
                            (response["accepted_id"], response["accepted_value"])
                        )
                else:
                    reason = response.get('reason') if response else 'Sem resposta'
                    print(f"[PAXOS {self.pid}] Prepare para {p} rejeitado ou falhou. Motivo: {reason}")
            
            # O próprio nó também conta como uma promessa
            print(f"[PAXOS {self.pid}] Fase Prepare: {len(promises) + 1} promessas recebidas.")
            if (len(promises) + 1) < quorum:
                print(f"[PAXOS {self.pid}] Quorum de promessas não atingido ({len(promises) + 1}/{quorum}). Proposta falhou.")
                return None

            # Decisão do valor a ser proposto na fase 2
            valor_a_propor = valor_inicial
            if accepted_values_from_peers:
                # Encontra a proposta com o maior ID (accepted_id) que foi retornada
                latest_proposal_id, latest_value = max(accepted_values_from_peers, key=lambda item: tuple(map(int, item[0].split('-'))))
                valor_a_propor = latest_value
                print(f"[PAXOS {self.pid}] Um valor previamente aceito ('{latest_proposal_id}') foi encontrado. Usando valor {valor_a_propor} em seu lugar.")
            else:
                print(f"[PAXOS {self.pid}] Nenhum valor previamente aceito. Usando valor inicial {valor_a_propor:.4f}.")

            # Fase 2: Accept
            accept_msg = {"type": "accept", "proposal_id": proposal_id, "value": valor_a_propor}
            accepted_by = []

            for p in promises:
                response = self.send(NODES[p], accept_msg)
                if response and response.get("ok"):
                    accepted_by.append(p)
                else:
                    reason = response.get('reason') if response else 'Sem resposta'
                    print(f"[PAXOS {self.pid}] Accept para {p} rejeitado ou falhou. Motivo: {reason}")
            
            # O próprio nó também aceita
            print(f"[PAXOS {self.pid}] Fase Accept: {len(accepted_by) + 1} nós aceitaram.")
            if (len(accepted_by) + 1) >= quorum:
                print(f"[PAXOS {self.pid}] Quorum de aceitação atingido! Valor {valor_a_propor} foi decidido.")
                return valor_a_propor
            else:
                print(f"[PAXOS {self.pid}] Quorum de aceitação não atingido ({len(accepted_by) + 1}/{quorum}). Consenso falhou.")
                return None
