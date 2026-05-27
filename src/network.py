import socket
import json
import threading

class NetworkManager:
    def __init__(self, port, peers, logger):
        self.port = port
        self.peers = peers
        self.logger = logger
        self.running = True

    def listen(self, callback):
        """ Inicia servidor TCP para escutar conexões e repassar mensagens """
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Permite reutilizar a porta rapidamente
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind(('0.0.0.0', self.port))
            server_socket.listen(5)
            # Define um timeout para que o loop possa checar self.running
            server_socket.settimeout(1.0)
            self.logger.info(f"Escutando conexões na porta {self.port}...")
            
            while self.running:
                try:
                    conn, addr = server_socket.accept()
                    # Lida com a conexão em uma thread separada para não bloquear outros receives
                    threading.Thread(target=self._handle_client, args=(conn, callback), daemon=True).start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        self.logger.error(f"Erro no listener socket: {e}")
                        
        finally:
            server_socket.close()

    def _handle_client(self, conn, callback):
        try:
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
                
            if data:
                message = json.loads(data.decode('utf-8'))
                callback(message)
        except Exception as e:
            self.logger.error(f"Erro ao processar mensagem recebida: {e}")
        finally:
            conn.close()

    def send_to(self, addr, message):
        """ Envia uma mensagem para um único endereço via TCP """
        host, port = addr
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(2.0)
            client_socket.connect((host, port))
            
            data = json.dumps(message).encode('utf-8')
            client_socket.sendall(data)
            client_socket.close()
            return True
        except ConnectionRefusedError:
            # Nó de destino pode estar offline
            return False
        except socket.timeout:
            # Timeout por rede ruim
            return False
        except Exception as e:
            return False

    def broadcast(self, message):
        """ Envia para todos os nós conhecidos (self.peers) """
        for peer_id, addr in self.peers.items():
            # Executamos o envio em uma nova thread para evitar travar num send_to que dê timeout
            threading.Thread(target=self.send_to, args=(addr, message), daemon=True).start()

    def stop(self):
        self.running = False
