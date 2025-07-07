import sys
import threading
import socket
import time
import random
import json
from node_config import NODES, NODES_LOCK
import multicast
from bully import Bully
from failure_detector import FailureDetector
from paxos import Paxos

# Estado global compartilhado com listas para mutabilidade em threads
rodada = [0]
leader = [None]
stop_event = threading.Event()
round_values = {}
round_values_lock = threading.Lock()

# Lock para acessar/modificar líder com segurança
leader_lock = threading.Lock()

# Estado local do acceptor Paxos
acceptor_state = {}  # Mapeia rodada -> dict com promised_id, accepted_id, accepted_value

lock_acceptor = threading.Lock()

def get_local_ip():
    """
    Função mais robusta para obter o IP local.
    Tenta se conectar a um IP público (como o DNS do Google) para descobrir
    qual interface de rede está sendo usada.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Não precisa enviar dados, só conectar
        s.connect(('8.8.8.8', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def calculate_i(pid):
    # Garantir que sempre retorne um float válido
    return random.random() * random.random() * pid or 0.0

def quorum_alive(pid, fd):
    # Adiciona o próprio PID à lista de nós vivos para o cálculo do quorum
    vivos = fd.get_alive_peers() + [pid]
    # O quorum é maioria simples do total de nós conhecidos
    with NODES_LOCK:
        total_nodes = len(NODES)
    return len(vivos) >= (total_nodes // 2 + 1)

def parse_proposal_id(proposal_id):
    """Converte o ID da proposta de string 'rodada-pid' para tupla (rodada, pid) de inteiros."""
    try:
        parts = proposal_id.split('-')
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return None, None


def socket_server(port):
    """Servidor TCP para responder mensagens Paxos e pings."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("", port)) # Bind em todas as interfaces
    s.listen()

    while not stop_event.is_set():
        try:
            conn, _ = s.accept()
            data = conn.recv(2048)
            if not data:
                conn.close()
                continue

            # Resposta para PING e ARE_YOU_ALIVE é fora do processamento JSON
            if data in [b"PING", b"ARE_YOU_ALIVE"]:
                conn.sendall(b"OK")
                conn.close()
                continue

            try:
                msg = json.loads(data.decode())
            except json.JSONDecodeError:
                print(f"[ERRO socket_server] Mensagem não-JSON recebida: {data}")
                conn.close()
                continue

            msg_type = msg.get("type")

            if msg_type == "prepare":
                proposal_id_str = msg.get("proposal_id")
                if not proposal_id_str:
                    conn.sendall(json.dumps({"ok": False, "error": "missing proposal_id"}).encode())
                    conn.close()
                    continue

                proposal_id_tuple = parse_proposal_id(proposal_id_str)
                rodada_id = proposal_id_tuple[0]

                with lock_acceptor:
                    if rodada_id not in acceptor_state:
                        acceptor_state[rodada_id] = {
                            "promised_id": None,
                            "accepted_id": None,
                            "accepted_value": None
                        }
                    promised_id_tuple = parse_proposal_id(acceptor_state[rodada_id]["promised_id"]) if acceptor_state[rodada_id]["promised_id"] else None

                    if promised_id_tuple is None or proposal_id_tuple >= promised_id_tuple:
                        acceptor_state[rodada_id]["promised_id"] = proposal_id_str
                        response = {
                            "ok": True,
                            "accepted_id": acceptor_state[rodada_id]["accepted_id"],
                            "accepted_value": acceptor_state[rodada_id]["accepted_value"]
                        }
                    else:
                        response = {"ok": False, "reason": f"Promised {promised_id_tuple} already"}
                conn.sendall(json.dumps(response).encode())

            elif msg_type == "accept":
                proposal_id_str = msg.get("proposal_id")
                value = msg.get("value")
                if not proposal_id_str or value is None:
                    conn.sendall(json.dumps({"ok": False, "error": "missing fields"}).encode())
                    conn.close()
                    continue

                proposal_id_tuple = parse_proposal_id(proposal_id_str)
                rodada_id = proposal_id_tuple[0]

                with lock_acceptor:
                    if rodada_id not in acceptor_state:
                        acceptor_state[rodada_id] = {
                            "promised_id": None,
                            "accepted_id": None,
                            "accepted_value": None
                        }
                    promised_id_tuple = parse_proposal_id(acceptor_state[rodada_id]["promised_id"]) if acceptor_state[rodada_id]["promised_id"] else None

                    if promised_id_tuple is None or proposal_id_tuple >= promised_id_tuple:
                        acceptor_state[rodada_id]["accepted_id"] = proposal_id_str
                        acceptor_state[rodada_id]["accepted_value"] = value
                        acceptor_state[rodada_id]["promised_id"] = proposal_id_str
                        response = {"ok": True}
                    else:
                        response = {"ok": False, "reason": f"Promised {promised_id_tuple} already"}
                conn.sendall(json.dumps(response).encode())

            else:
                conn.sendall(b"UNKNOWN")

            conn.close()

        except Exception as e:
            print(f"[ERRO socket_server] {e}")


def make_multicast_callback(pid, port):
    def callback(data, addr):
        multicast.process_multicast_message(data, addr, pid, port, rodada, leader)
    return callback

def send_heartbeats(pid, port):
    while not stop_event.is_set():
        # Não precisa do lock aqui, pois a leitura de valores atômicos é segura
        multicast.send_heartbeat(pid, port, rodada[0], leader[0])
        time.sleep(5)

from multicast import sync_responses, sync_lock, send_sync_request

def run_sync_protocol(pid, port):
    with sync_lock:
        sync_responses.clear()

    print("[SYNC] Enviando pedido de sincronização...")
    send_sync_request(pid, port)
    time.sleep(3) # Aguarda respostas

    with sync_lock:
        respostas = list(sync_responses)

    if not respostas:
        print("[SYNC] Nenhuma resposta recebida. Assumindo rodada 1 e elegendo novo líder.")
        return 1, None # Retorna None para forçar uma eleição

    # Lógica de votação para decidir o estado mais comum
    votos = {}
    for r in respostas:
        key = (r["rodada"], r["leader"])
        votos[key] = votos.get(key, 0) + 1

    # Escolhe o estado com mais votos
    rodada_resp, lider_resp = max(votos.items(), key=lambda x: x[1])[0]
    print(f"[SYNC] Protocolo de sincronia concluído. Rodada: {rodada_resp}, Líder: {lider_resp}")
    return rodada_resp, lider_resp

rodada_lock = threading.Lock()

def main(pid_arg, port_arg):
    global pid, port
    pid = pid_arg
    port = port_arg
    last_computed_round = -1
    local_ip = get_local_ip()

    print(f"[STARTUP] PID={pid} PORT={port} IP={local_ip}")

    # Adiciona o próprio nó ao dicionário ANTES de iniciar qualquer comunicação
    with NODES_LOCK:
        NODES[pid] = (local_ip, port)

    # Inicia os serviços de background
    threading.Thread(target=multicast.multicast_listener, args=(make_multicast_callback(pid, port), stop_event), daemon=True).start()
    threading.Thread(target=send_heartbeats, args=(pid, port), daemon=True).start()
    threading.Thread(target=socket_server, args=(port,), daemon=True).start()

    # Sincroniza com a rede para obter estado atual
    rodada_atual, lider_atual = run_sync_protocol(pid, port)
    with leader_lock:
        rodada[0] = rodada_atual
        leader[0] = lider_atual

    # Se não há líder após a sincronia, faz uma eleição
    with leader_lock:
        if leader[0] is None:
            print("[SYNC] Nenhum líder encontrado na rede, iniciando eleição.")
            leader[0] = Bully(pid).run_election()
    
    print(f"[START] Estado inicial: Rodada {rodada[0]}, Líder {leader[0]}")
    
    # Inicia o detector de falhas após a configuração inicial
    def on_fail(dead_pid):
        print(f"[FD] Detectada falha no nó {dead_pid}.")
        with leader_lock:
            if dead_pid == leader[0]:
                print(f"[FD] O líder {dead_pid} caiu. Iniciando nova eleição...")
                leader[0] = Bully(pid).run_election()
                print(f"[NEW LEADER] Novo líder eleito: {leader[0]}")

    fd = FailureDetector(pid, on_fail)
    fd.start()

    paxos = Paxos(pid)

    # Aguarda a descoberta de um número mínimo de nós (ex: 3)
    # Em um cenário real, o número pode vir de um arquivo de configuração
    while True:
        with NODES_LOCK:
            if len(NODES) >= 3:
                print(f"[{pid}] Nós suficientes descobertos ({len(NODES)}). Iniciando loop principal.")
                break
        print(f"[{pid}] Aguardando descoberta de mais nós... Atuais: {len(NODES)}")
        time.sleep(3)

    while not stop_event.is_set():
        current_leader = leader[0]
        if pid == current_leader:
            with rodada_lock:
                current_round = rodada[0]
                if current_round > last_computed_round:
                    if not quorum_alive(pid, fd):
                        print(f"[{pid}] Quorum insuficiente, pulando rodada {current_round}...")
                        time.sleep(5)
                        continue

                    # Calcula o valor para a rodada atual
                    valor_proposto = calculate_i(pid)
                    print(f"[{pid}] Sou o líder. Propondo valor {valor_proposto:.4f} para a rodada {current_round}.")
                    
                    # Inicia o consenso Paxos
                    resultado = paxos.propose(current_round, valor_proposto)

                    if resultado is not None:
                        print(f"[{pid}][SUCCESS] Rodada {current_round}: Consenso alcançado com valor = {resultado}")
                        # Notifica todos sobre o novo estado (avanço de rodada)
                        multicast.send_sync_update(pid, current_round + 1, leader[0])
                        rodada[0] += 1
                    else:
                        print(f"[{pid}][FAILURE] Rodada {current_round}: Falha no consenso. Tentando novamente na próxima iteração.")

                    last_computed_round = current_round
                    time.sleep(10) # Intervalo entre rodadas
                else:
                    time.sleep(1) # Espera se a rodada não avançou
        else:
            # Nós não-líderes apenas aguardam atualizações
            # O timeout no sleep permite que o programa termine mais rápido se stop_event for setado
            time.sleep(2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python3 main.py <PID> <PORTA>")
        sys.exit(1)
    try:
        pid_arg = int(sys.argv[1])
        port_arg = int(sys.argv[2])
        main(pid_arg, port_arg)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Encerrando o nó...")
        stop_event.set()
    except Exception as e:
        print(f"[FATAL] Erro inesperado: {e}")
        stop_event.set()
