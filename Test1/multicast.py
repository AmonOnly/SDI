import socket
import struct
import threading
import json
import time

# CORREÇÃO: Importa o lock global correto para o dicionário NODES
from node_config import MULTICAST_GROUP, MULTICAST_PORT, BUFFER_SIZE, NODES, NODES_LOCK

# Variáveis para o protocolo de sincronização
sync_responses = []
sync_lock = threading.Lock()

def send_multicast_message(msg_dict):
    """Envia uma mensagem para o grupo multicast."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        msg = json.dumps(msg_dict).encode('utf-8')
        sock.sendto(msg, (MULTICAST_GROUP, MULTICAST_PORT))
        sock.close()
    except Exception as e:
        print(f"[ERRO send_multicast_message] {e}")

def multicast_listener(callback, stop_event):
    """Ouve por mensagens no grupo multicast e chama o callback."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', MULTICAST_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(1.0) # Timeout para permitir a verificação do stop_event

        while not stop_event.is_set():
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                callback(data, addr)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[ERRO multicast_listener] {e}")

def process_multicast_message(data, addr, pid, port, rodada, leader):
    """Processa uma mensagem multicast recebida."""
    try:
        msg = json.loads(data.decode('utf-8'))
        msg_type = msg.get("type")
        sender_pid = int(msg.get("pid"))

        # Ignora mensagens do próprio nó
        if sender_pid == pid:
            return

        # Atualiza a lista de nós conhecidos se a porta for válida
        sender_port = int(msg.get("port", 0))
        if sender_port > 0:
            # CORREÇÃO: Usa o lock global e correto (NODES_LOCK)
            with NODES_LOCK:
                # Atualiza o endereço do nó apenas se for diferente, para evitar escritas desnecessárias
                if NODES.get(sender_pid) != (addr[0], sender_port):
                    NODES[sender_pid] = (addr[0], sender_port)
                    print(f"[DISCOVERY] Nó {sender_pid} descoberto/atualizado em {addr[0]}:{sender_port}")


        if msg_type == "SYNC_REQUEST":
            # Um nó pediu para sincronizar. Responda com o estado atual.
            response = {
                "type": "SYNC_RESPONSE",
                "pid": pid,
                "port": port,
                "rodada": rodada[0],
                "leader": leader[0]
            }
            send_multicast_message(response)

        elif msg_type == "SYNC_RESPONSE":
            # Resposta a um pedido de sincronia. Adiciona à lista para processamento.
            with sync_lock:
                # Evita adicionar respostas duplicadas do mesmo nó
                if not any(r['pid'] == sender_pid for r in sync_responses):
                    sync_responses.append({
                        "pid": sender_pid,
                        "rodada": msg.get("rodada"),
                        "leader": msg.get("leader")
                    })

        elif msg_type == "SYNC_UPDATE":
            # O líder notificou um avanço de rodada após um consenso.
            new_rodada = msg.get("rodada")
            new_leader = msg.get("leader")
            if new_rodada is not None and new_leader is not None:
                if rodada[0] != new_rodada or leader[0] != new_leader:
                    print(f"[SYNC_UPDATE] Atualizando estado. Rodada: {rodada[0]}->{new_rodada}, Líder: {leader[0]}->{new_leader}")
                    rodada[0] = new_rodada
                    leader[0] = new_leader
        
        # O tipo "HEARTBEAT" é recebido mas não requer ação aqui.
        # Ele serve para que outros nós atualizem a lista `NODES`.

    except (json.JSONDecodeError, UnicodeDecodeError, ValueError, KeyError) as e:
        # Ignora mensagens malformadas
        # print(f"[ERRO process_multicast_message] Mensagem inválida de {addr}: {e}")
        pass
    except Exception as e:
        print(f"[ERRO process_multicast_message] {e}")


def send_sync_request(pid, port):
    """Envia uma mensagem para descobrir o estado da rede."""
    msg = {"type": "SYNC_REQUEST", "pid": pid, "port": port}
    send_multicast_message(msg)

def send_sync_update(pid, rodada, leader):
    """Envia uma atualização de estado para todos os nós (usado pelo líder)."""
    msg = {"type": "SYNC_UPDATE", "pid": pid, "port": 0, "rodada": rodada, "leader": leader}
    send_multicast_message(msg)

def send_heartbeat(pid, port, rodada, leader):
    """Envia um pulso para mostrar que o nó está ativo."""
    msg = {"type": "HEARTBEAT", "pid": pid, "port": port, "rodada": rodada, "leader": leader}
    send_multicast_message(msg)
