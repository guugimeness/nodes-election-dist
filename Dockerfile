# Utiliza Python 3.11+ conforme exigido
FROM python:3.11-slim 

# Instala o iproute2 para habilitar o comando 'tc' usado na injeção de atrasos
RUN apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho
WORKDIR /app

# Não há dependências externas (ZeroMQ, etc), usando apenas biblioteca padrão
# Copia o código-fonte da aplicação
COPY . .

# Comando padrão de inicialização (substitua 'node.py' pelo nome do seu script principal)
CMD ["python", "src/node.py"]