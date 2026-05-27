import os
import threading

# Módulos do sistema
from election import BullyElection
from network import NetworkManager
from timeout import AdaptiveTimeout
from logger import NodeLogger
from clock import NodeClock

class Node:
    def __init__(self):
        self.node_id = int(os.environ.get('NODE_ID'))
        self.port = int(os.environ.get('PORT', 5000))
        drift_ms = int(os.environ.get('CLOCK_DRIFT_MS', 0))
        
        self.clock = NodeClock(drift_ms)
        self.logger = NodeLogger(self.node_id, self.clock)
        
        # Mapeia os pares: ID do Nó -> 'host:porta'
        raw_peers = os.environ.get('PEERS', '').split(',')
        self.peers = {}
        for peer_str in raw_peers:
            if peer_str:
                host, port = peer_str.split(':')
                # Simplificação: Usamos o último caractere do hostname (ex: node1 -> 1) como ID
                peer_id = int(host[-1])
                # Remove a si próprio da lista de peers para não enviar mensagens para si
                if peer_id != self.node_id:
                    self.peers[str(peer_id)] = (host, int(port))

        # Inicializa os módulos
        self.network = NetworkManager(self.port, self.peers, self.logger)
        self.timeout_mgr = AdaptiveTimeout(self.clock)
        
        # Injeta o algoritmo Bully
        self.election = BullyElection(self.node_id, self.network, self.logger, self.clock, self.timeout_mgr)

        # Estado da máquina
        self.state = 'CANDIDATE' # Começa como candidato para iniciar a eleição logo no boot
        self.leader_id = None
        self.running = True

    def start(self):
        self.logger.info(f"Nó {self.node_id} inicializado com drift de {self.clock.drift_rate*1000} ms/s.")
        
        # Escuta conexões TCP em background
        listener_thread = threading.Thread(target=self.network.listen, args=(self.handle_message,))
        listener_thread.daemon = True
        listener_thread.start()

        self.run_state_machine()

    def handle_message(self, message):
        """ Roteia a mensagem recebida para o módulo correto """
        msg_type = message.get('type')

        # Se for heartbeat (coordenador enviando)
        if msg_type == 'HEARTBEAT':
            # Só aceita heartbeat se vier de um líder válido
            sender_id = message.get('sender_id')
            # Se for um ex-lider com id menor e a gente já tem outro, podemos ignorar, 
            # mas o bully resolve isso. Vamos aceitar e resetar timer.
            if self.leader_id is None or sender_id >= self.leader_id:
                if self.state != 'FOLLOWER' or self.leader_id != sender_id:
                    self.logger.info(f"Reconhecendo Nó {sender_id} como líder via HEARTBEAT.")
                self.leader_id = sender_id
                self.state = 'FOLLOWER'
                if self.timeout_mgr.record_heartbeat():
                    self.logger.debug(f"Atraso na rede variou! Timeout adaptativo recalibrado para {self.timeout_mgr.get_estimated_timeout():.2f}s")
                self.logger.debug(f"Recebi HEARTBEAT regular do líder {sender_id}")
        
        # Se for mensagem do tipo Bully (ELECTION, ALIVE, COORDINATOR)
        elif msg_type in ['ELECTION', 'ALIVE', 'COORDINATOR']:
            novo_estado, novo_lider = self.election.handle_message(message, self.state, self.peers)
            
            if novo_estado:
                self.state = novo_estado
            if novo_lider:
                self.leader_id = novo_lider
                self.timeout_mgr.reset_timer()

    def run_state_machine(self):
        """ Loop dinâmico do nó """
        while self.running:
            if self.state == 'LEADER':
                # Comportamento do seu nó 'coordinator'
                self.logger.debug("Enviando HEARTBEAT para todos os nós (Broadcast).")
                self.network.broadcast({'type': 'HEARTBEAT', 'sender_id': self.node_id})
                # O intervalo de envio precisa ser menor que o timeout. 
                # Vamos enviar a cada 1.5s (Tempo lógico)
                self.clock.sleep(1.5) 
            
            elif self.state == 'FOLLOWER':
                # Comportamento do seu 'detector_process'
                if self.timeout_mgr.has_timed_out():
                    self.logger.warning("Falha do líder detectada pelo timeout adaptativo! Iniciando eleição...")
                    self.state = 'CANDIDATE'
                    self.leader_id = None
                    self.timeout_mgr.reset_timer()
                
                self.clock.sleep(0.5)
            
            elif self.state == 'CANDIDATE':
                # Chama a rotina Bully que escrevemos
                novo_estado, novo_lider = self.election.start_election(self.peers)
                self.state = novo_estado
                self.leader_id = novo_lider
                
                # Se após a eleição ainda somos candidatos (ex: empate ou falha na rede), evitamos spam de eleições
                if self.state == 'CANDIDATE':
                    self.clock.sleep(1)

if __name__ == '__main__':
    try:
        node = Node()
        node.start()
    except KeyboardInterrupt:
        print("Encerrando...")