class BullyElection:
    def __init__(self, node_id, network_manager, logger, clock, timeout_mgr):
        self.node_id = int(node_id)
        self.network = network_manager
        self.logger = logger
        self.clock = clock
        self.timeout_mgr = timeout_mgr
        self.waiting_for_alive = False

    def start_election(self, peers):
        """ Inicia a eleição conforme a lógica Bully """
        self.logger.info(f"[BULLY] Nó {self.node_id} iniciando eleição...")
        
        # Encontra nós com prioridade (ID) maior que a minha
        higher_priority_peers = {peer_id: addr for peer_id, addr in peers.items() if int(peer_id) > self.node_id}

        if not higher_priority_peers:
            # Se não há ninguém com prioridade maior, eu sou o coordenador
            self.logger.info(f"[BULLY] Nenhuma prioridade maior. Eu ({self.node_id}) sou o novo Coordenador!")
            self.announce_coordinator(peers)
            return 'LEADER', self.node_id

        # Envia mensagem 'election' para todos com ID maior
        self.waiting_for_alive = True
        for peer_id, addr in higher_priority_peers.items():
            self.logger.debug(f"[BULLY] Enviando 'election' para nó {peer_id}")
            self.network.send_to(addr, {'type': 'ELECTION', 'sender_id': self.node_id})

        # Aguarda um tempo adaptativo para receber 'alive'
        # Usamos o timeout_mgr para saber qual o RTT estimado atual da rede,
        # adicionando um bônus por segurança em época de sobrecarga (eleição)
        wait_time = self.timeout_mgr.get_estimated_timeout()
        self.clock.sleep(wait_time) 

        if self.waiting_for_alive:
            # Se ninguém respondeu 'alive' a tempo, assumimos a liderança
            self.logger.warning(f"[BULLY] Nenhum nó superior respondeu em {wait_time:.2f}s. Assumindo a liderança.")
            self.announce_coordinator(peers)
            return 'LEADER', self.node_id
        else:
            # Alguém respondeu 'alive', então voltamos a ser seguidores esperando o anúncio
            self.logger.info(f"[BULLY] Nó superior respondeu 'ALIVE'. Aguardando anúncio de coordenador.")
            return 'FOLLOWER', None

    def handle_message(self, message, current_state, peers):
        """ Processa as mensagens de rede específicas da eleição """
        msg_type = message.get('type')
        sender_id = int(message.get('sender_id'))

        if msg_type == 'ELECTION':
            # Se alguém de prioridade MENOR chamou eleição, respondemos 'alive' e começamos nossa eleição
            if sender_id < self.node_id:
                self.logger.debug(f"[BULLY] Recebi 'election' de {sender_id}. Respondendo 'alive'.")
                sender_addr = peers.get(str(sender_id))
                if sender_addr:
                    self.network.send_to(sender_addr, {'type': 'ALIVE', 'sender_id': self.node_id})
                
                # Inicia a própria eleição logo em seguida (vira candidato)
                return 'CANDIDATE', None
                
        elif msg_type == 'ALIVE':
            # Um nó superior disse que está vivo, cancelamos nossa intenção de ser líder
            if sender_id > self.node_id:
                self.logger.debug(f"[BULLY] Recebi 'alive' do nó {sender_id}.")
                self.waiting_for_alive = False
                return 'FOLLOWER', None

        elif msg_type == 'COORDINATOR':
            # Alguém se declarou líder
            self.logger.info(f"[BULLY] Nó {sender_id} assumiu como Coordenador.")
            self.waiting_for_alive = False
            return 'FOLLOWER', sender_id

        return current_state, None

    def announce_coordinator(self, peers):
        """ Espalha a mensagem 'coordinator:id' """
        msg = {'type': 'COORDINATOR', 'sender_id': self.node_id}
        self.network.broadcast(msg)