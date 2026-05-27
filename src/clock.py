import time

class NodeClock:
    """
    Simula um relógio local com uma deriva artificial (clock drift).
    drift_ms_per_second: Quantos milissegundos o relógio avança ou atrasa a cada segundo real.
    """
    def __init__(self, drift_ms_per_second=0):
        self.drift_rate = float(drift_ms_per_second) / 1000.0
        self.start_real_time = time.time()
        self.start_logical_time = self.start_real_time

    def time(self):
        """ Retorna o tempo com a deriva aplicada """
        elapsed_real = time.time() - self.start_real_time
        drifted_elapsed = elapsed_real * (1.0 + self.drift_rate)
        return self.start_logical_time + drifted_elapsed

    def sleep(self, seconds):
        """ Dorme por 'seconds' segundos no tempo lógico """
        # Se eu quero que passe 'seconds' lógicos, quantos reais devem passar?
        # drifted_elapsed = elapsed_real * (1 + drift) => elapsed_real = drifted_elapsed / (1 + drift)
        real_seconds = seconds / (1.0 + self.drift_rate)
        time.sleep(max(0, real_seconds))
