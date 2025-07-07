import threading

# Configurações do grupo Multicast
MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT = 5007
BUFFER_SIZE = 2048 # Aumentado para acomodar mensagens JSON maiores

# Dicionário dinâmico que armazena o estado dos nós conhecidos.
# Formato: { pid (int): (ip (str), porta (int)) }
NODES = {}

# Lock global para acesso seguro ao dicionário NODES em múltiplas threads.
# QUALQUER operação de leitura ou escrita em NODES deve adquirir este lock.
NODES_LOCK = threading.Lock()
