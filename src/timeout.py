class AdaptiveTimeout:
    def __init__(self, clock):
        self.clock = clock
        self.last_heartbeat_time = self.clock.time()
        self.ewma_interval = 2.0 # Inicialmente assume o tempo fixo do heartbeat do líder
        self.alpha = 0.125
        self.variance = 0.0

    def record_heartbeat(self):
        current_time = self.clock.time()
        interval = current_time - self.last_heartbeat_time
        
        # Se o intervalo for absurdamente grande (ex: ficou muito tempo sem receber e trocou de líder)
        # nós limitamos para não distorcer o EWMA
        if interval > 10.0:
            interval = 2.0
            
        self.last_heartbeat_time = current_time
        
        # Cálculo inspirado na RFC 6298 para RTT
        error = interval - self.ewma_interval
        self.ewma_interval = self.ewma_interval + self.alpha * error
        self.variance = (1 - self.alpha) * self.variance + self.alpha * abs(error)
        
        # Retorna True se houve uma variação notável para podermos printar no log
        return abs(error) > 0.1

    def reset_timer(self):
        """ Reseta o cronômetro sem recalcular as médias (útil ao iniciar eleição ou assumir folower) """
        self.last_heartbeat_time = self.clock.time()

    def get_estimated_timeout(self):
        """ Retorna o limite de tempo calculado com base nas médias e variância, mais uma margem de segurança """
        # Estimativa do intervalo + 4x a variância + margem de erro por delays de processamento
        return self.ewma_interval + 4 * self.variance + 1.5

    def has_timed_out(self):
        timeout_threshold = self.get_estimated_timeout()
        current_time = self.clock.time()
        return (current_time - self.last_heartbeat_time) > timeout_threshold
