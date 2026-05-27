# Eleição de Líder Resiliente (Grupo B)

Um sistema distribuído implementado em Python, com o objetivo de eleger um único coordenador confiável no meio de uma rede turbulenta (com *delays* variados e relógios sem sincronia).

## Estratégias de Implementação

* **Eleição (Algoritmo Bully):** O nó de maior ID disponível sempre se torna o líder. Para evitar bloqueios na máquina de estados principal, as mensagens de eleição/heartbeats (via **Sockets TCP**) são recebidas em uma *thread* separada rodando em background.
* **Tolerância a Atrasos:** Descartamos os clássicos *timeouts fixos*. O detector de falhas utiliza **Timeout Adaptativo (EWMA)**, que mede a velocidade da rede em tempo real e estica o "limite de tolerância" se a rede ficar lenta, prevenindo falsas eleições.
* **Relógios Inconsistentes:** A classe `NodeClock` intercepta todas as requisições de tempo, avançando ou atrasando os "segundos" baseando-se no `CLOCK_DRIFT_MS` de cada máquina, forçando a lógica a sobreviver em fusos horários fragmentados.

## Sistema de Logs

Os containers operam sem poluir o terminal principal. Tudo o que acontece é salvo automaticamente em formato texto.
* Acesse a pasta `logs/` na raiz do projeto.
* Nela, você encontrará arquivos individuais (`node_1.txt`, `node_2.txt`, etc). 
* Esses arquivos detalham, com o relógio driftado daquele nó específico: a chegada de *heartbeats*, início de eleições, recalibramento matemático do atraso e detecções de queda de líder.

## Como Testar

Como pertencemos ao Grupo B, nossa base de testes de atrasos (*delays*) deve utilizar **múltiplos de 2**.

**1. Suba a rede em Background:**
```bash
docker-compose up -d --build
```
*(Confira a pasta `logs/` para ver o Nó 4, que possui o maior ID, assumindo a liderança inicial).*

**2. Provoque a Falha do Líder Atual:**
```bash
docker stop node4
```
*(Abra o arquivo `logs/node_3.txt` para vê-lo detectando a falha, superando os nós 1 e 2, e se auto-elegendo).*

**3. Injete a Turbulência na Rede (Múltiplo de 2):**
Coloque um gargalo terrível na rede do Nó 2 (400ms de delay + variação caótica de 200ms):
```bash
docker exec node2 tc qdisc add dev eth0 root netem delay 400ms 200ms
```
*(Abra o log do Nó 2. Você verá as mensagens em `[DBG]` avisando que a rede variou e o Timeout Adaptativo foi recalculado automaticamente e esticado para não colapsar a rede. Para remover o teste depois, troque o comando `add` por `del`).*
