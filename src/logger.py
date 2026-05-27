import datetime
import sys
import os

class NodeLogger:
    def __init__(self, node_id, clock):
        self.node_id = node_id
        self.clock = clock
        self.log_file = f"logs/node_{self.node_id}.txt"
        
        # Garante que a pasta logs existe (caso não seja montada)
        os.makedirs("logs", exist_ok=True)

    def _log(self, level, message):
        # Usar o relógio com drift para pegar o timestamp
        timestamp = self.clock.time()
        # Formata como HH:MM:SS.mmm
        dt = datetime.datetime.fromtimestamp(timestamp)
        time_str = dt.strftime('%H:%M:%S.%f')[:-3]
        
        log_line = f"[{time_str}] [{level}] [Node {self.node_id}] {message}"
        print(log_line, flush=True)
        
        # Publica no txt da pasta logs
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_line + "\n")
        except Exception as e:
            print(f"Erro ao escrever no log: {e}", file=sys.stderr)

    def info(self, message):
        self._log('INFO', message)

    def warning(self, message):
        self._log('WARN', message)

    def error(self, message):
        self._log('ERR ', message)

    def debug(self, message):
        self._log('DBG ', message)
