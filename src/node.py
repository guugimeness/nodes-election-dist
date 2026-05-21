import os
import time
import threading

# Módulos do sistema
from election import BullyElection
from network import NetworkManager # (Que deve envelopar o envio via socket que você já sabe fazer)
from timeout import AdaptiveTimeout
from logger import NodeLogger

class Node:
    def __init__(self):
        # 1. Carrega dados via Variáveis de Ambiente do Docker (Substituindo o argparse e Manager)
        self.node_id = int(os.environ.get('NODE_ID'))
        self.port = int(os.environ.get('PORT', 5000))
        
        # Mapeia os pares: ID do Nó -> 'host:porta'
        raw_peers = os.environ.get('PEERS', '').split(',')
        self.peers = {}
        for peer_str in raw_peers:
            if peer_str:
                host, port = peer_str.split(':')
                # Simplificação: Usamos o último caractere do hostname (ex: node1 -> 1) como ID
                peer_id = host[-1] 
                self.peers[peer_id] = (host, int(port))

        # 2. Inicializa os módulos
        self.logger = NodeLogger(self.node_id)
        self.network = NetworkManager(self.port, self.peers, self.logger)
        self.timeout_mgr = AdaptiveTimeout()
        
        # Injeta o algoritmo Bully
        self.election = BullyElection(self.node_id, self.network, self.logger)

        # 3. Estado da máquina
        self.state = 'CANDIDATE' # Começa como candidato para iniciar a eleição logo no boot
        self.leader_id = None
        self.running = True

    def start(self):
        self.logger.info(f"Nó {self.node_id} inicializado.")
        
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
            self.leader_id = message.get('sender_id')
            self.state = 'FOLLOWER'
            self.timeout_mgr.reset_timer()
        
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
                self.network.broadcast({'type': 'HEARTBEAT', 'sender_id': self.node_id})
                time.sleep(2) # Intervalo do Heartbeat
            
            elif self.state == 'FOLLOWER':
                # Comportamento do seu 'detector_process'
                if self.timeout_mgr.has_timed_out():
                    self.logger.warning("Falha do líder detectada! Iniciando eleição...")
                    self.state = 'CANDIDATE'
                time.sleep(0.5)
            
            elif self.state == 'CANDIDATE':
                # Chama a rotina Bully que escrevemos
                novo_estado, novo_lider = self.election.start_election(self.peers)
                self.state = novo_estado
                self.leader_id = novo_lider
                time.sleep(1) # Previne spam de eleições caso ocorra uma disputa

if __name__ == '__main__':
    node = Node()
    node.start()