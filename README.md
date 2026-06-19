
# Sistema Distribuído Completo

## Requisitos implementados

- RPC real com gRPC
- Sincronização de relógios físicos
- Relógios lógicos de Lamport
- Exclusão mútua distribuída
- Eleição de líder
- Paralelismo distribuído

## Instalação

### 1. Criar ambiente virtual

```bash
python -m venv .venv
```

### 2. Iniciar ambiente virtual

```bash
.venv\Scripts\activate
```

### 3. Instalar dependências

```bash
pip install -r requirements.txt
```

### 4. Gerar arquivos gRPC

```bash
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/genetic.proto
```

## Executar

Abra três terminais e para cada um execute os seguintes comandos:

Terminal 1:

```bash
.venv\Scripts\activate
python node/node.py 1 5001
```

Terminal 2:

```bash
.venv\Scripts\activate
python node/node.py 2 5002
```

Terminal 3:

```bash
.venv\Scripts\activate
python node/node.py 3 5003
```

## O que observar

- eleição do líder
- sincronização de relógios
- envio RPC real
- exclusão mútua
- atualização do relógio lógico
