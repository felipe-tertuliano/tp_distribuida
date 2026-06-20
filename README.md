# Sistema Distribuído de Algoritmo Genético

## 📋 Visão Geral

Sistema distribuído para evolução paralela de algoritmos genéticos, onde múltiplos nós colaboram para otimizar soluções através de troca de indivíduos e coordenação distribuída. O sistema implementa protocolos de consenso e sincronização para garantir consistência e tolerância a falhas.

## 🚀 Funcionalidades

### Técnicas Distribuídas

- **Sincronização de Relógios**: Relógios lógicos de Lamport para ordenação causal e sincronização de relógios físicos com o líder
- **Algoritmo Genético**: Evolução paralela com compartilhamento de indivíduos entre nós para diversidade genética
- **Eleição de Líder**: Protocolo Bully para eleição automática do nó coordenador
- **Tolerância a Falhas**: Detecção e remoção automática de nós inativos com timeout configurável
- **Two-Phase Commit (2PC)**: Protocolo de consenso distribuído para garantir consistência nas operações

### Tecnologias

- **gRPC**: Comunicação eficiente entre nós com chamadas de procedimento remoto
- **Protocolo UDP**: Descoberta de nós e mensagens de presença na rede

## 📦 Instalação

### 1. Criar ambiente virtual

```bash
python -m venv .venv
```

### 2. Ativar ambiente virtual

**Windows:**

```bash
.venv\Scripts\activate
```

**Linux/macOS:**

```bash
source venv/bin/activate
```

### 3. Instalar dependências

```bash
pip install -r requirements.txt
```

### 4. Gerar arquivos gRPC

```bash
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/genetic.proto
```

## 🏃 Executando o Sistema

Abra terminais para cada nó (mínimo 2 para ver distribuição):

**Terminal 1:**

```bash
python node/node.py 1
```

**Terminal 2:**

```bash
python node/node.py 2
```

**Terminal 3:**

```bash
python node/node.py 3
```

e assim por diante...

## 📊 O que Observar

- **Eleição do Líder**: Nó com maior ID assume automaticamente a liderança
- **Sincronização de Relógios**: Relógios lógicos e físicos mantidos consistentes entre nós
- **Comunicação RPC**: Chamadas remotas entre nós via gRPC
- **Exclusão Mútua**: Acesso coordenado a recursos compartilhados
- **Evolução da População**: Melhoria contínua dos indivíduos em cada geração
- **Compartilhamento 2PC**: Troca de soluções entre nós com garantia de consistência

## 📝 Arquitetura

### Componentes Principais

1. **NodeConfig**: Configurações centralizadas do nó (portas, timeouts, tamanhos)
2. **NodeState**: Gerenciamento de estado (população, relógios, nós ativos)
3. **DiscoverySystem**: Descoberta e manutenção de nós na rede via UDP
4. **ClockManager**: Sincronização de relógios lógicos e físicos
5. **TwoPhaseCommit**: Protocolo de consenso distribuído para operações atômicas
6. **CriticalSectionManager**: Controle de acesso a recursos compartilhados
7. **GeneticAlgorithmManager**: Operações do algoritmo genético (evolução, mutação)
8. **GeneticService**: Serviço gRPC com implementação dos métodos remotos
