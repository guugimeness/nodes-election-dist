# Utiliza Python 3.11+ conforme exigido
FROM python:3.11-slim 

# Instala o iproute2 para habilitar o comando 'tc' usado na injeção de atrasos
RUN apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho
WORKDIR /app

# Copia os arquivos de dependência (se houver, ex: pyzmq) e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || echo "Nenhum requirements.txt fornecido"

# Copia o código-fonte da aplicação
COPY . .

# Comando padrão de inicialização (substitua 'node.py' pelo nome do seu script principal)
CMD ["python", "node.py"]