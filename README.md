
# Sistema Distribuído Completo

## Requisitos implementados

- RPC real com gRPC
- Sincronização de relógios físicos
- Relógios lógicos de Lamport
- Exclusão mútua distribuída
- Eleição de líder
- Paralelismo distribuído

## Instalação

```bash
pip install grpcio grpcio-tools
```

## Gerar arquivos gRPC

```bash
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/genetic.proto
```

## Executar

Terminal 1:

```bash
python node/node.py 1 5001
```

Terminal 2:

```bash
python node/node.py 2 5002
```

Terminal 3:

```bash
python node/node.py 3 5003
```

## O que observar

- eleição do líder
- sincronização de relógios
- envio RPC real
- exclusão mútua
- atualização do relógio lógico
