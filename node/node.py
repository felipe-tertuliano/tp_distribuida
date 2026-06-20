import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from genetic import create_individual, fitness, mutate
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from contextlib import contextmanager
from concurrent import futures
from enum import Enum

import genetic_pb2_grpc
import genetic_pb2
import threading
import logging
import random
import socket
import time
import grpc
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============= Configuração =============
@dataclass
class NodeConfig:
    """Configuração centralizada do nó"""
    discovery_port: int = 9999
    discovery_interval: int = 5
    node_timeout: int = 15
    base_port: int = 5000
    grpc_threads: int = 10
    population_size: int = 10
    max_population: int = 100
    generation_interval: int = 3
    share_interval: int = 5
    prepare_timeout: float = 5.0
    
    @classmethod
    def from_args(cls, node_id: int) -> 'NodeConfig':
        """Cria configuração com valores específicos do nó"""
        config = cls()
        config.node_id = node_id
        config.port = cls.base_port + node_id
        return config

# ============= Gerenciamento de Estado =============
@dataclass
class NodeState:
    """Gerenciamento centralizado de estado"""
    node_id: int
    config: NodeConfig
    logical_clock: int = 0
    clock_offset: float = 0
    generation: int = 0
    transaction_counter: int = 0
    resource_busy: bool = False
    
    # Coleções thread-safe
    active_nodes: Dict[int, Dict] = field(default_factory=dict)
    pending_transactions: Dict[int, Dict] = field(default_factory=dict)
    population: List[List[float]] = field(default_factory=list)
    
    # Locks
    nodes_lock: threading.Lock = field(default_factory=threading.Lock)
    leader_lock: threading.Lock = field(default_factory=threading.Lock)
    transaction_lock: threading.Lock = field(default_factory=threading.Lock)
    critical_section_lock: threading.Lock = field(default_factory=threading.Lock)
    
    def __post_init__(self):
        self._leader_id: Optional[int] = None
        self.population = [create_individual() for _ in range(self.config.population_size)]
    
    @property
    def leader_id(self) -> Optional[int]:
        with self.leader_lock:
            return self._leader_id
    
    @leader_id.setter
    def leader_id(self, value: Optional[int]):
        with self.leader_lock:
            self._leader_id = value
            if value is not None:
                logger.info(f"[Nó {self.node_id}{" | LÍDER" if self.node_id == value else ""}] Líder definido: Nó {value}")
            else:
                logger.info(f"[Nó {self.node_id}] Líder removido")
    
    def get_log_prefix(self) -> str:
        """Retorna prefixo padronizado para logs"""
        is_leader = self.leader_id == self.node_id
        return f"[Nó {self.node_id}{" | LÍDER" if is_leader else ""}]"

# ============= Funções Utilitárias =============
class MessageType(Enum):
    """Tipos de mensagem para comunicação em rede"""
    DISCOVERY = 'discovery'
    DISCOVERY_RESPONSE = 'discovery_response'
    PRESENCE = 'presence'

def create_message(msg_type: MessageType, node_id: int, port: int, **kwargs) -> dict:
    """Cria uma mensagem padronizada"""
    message = {
        'type': msg_type.value,
        'node_id': node_id,
        'port': port,
        'timestamp': time.time(),
        **kwargs
    }
    return message

def serialize_message(message: dict) -> bytes:
    """Serializa mensagem para bytes JSON"""
    return json.dumps(message).encode()

def deserialize_message(data: bytes) -> Optional[dict]:
    """Desserializa mensagem JSON com segurança"""
    try:
        return json.loads(data.decode())
    except json.JSONDecodeError:
        return None

@contextmanager
def udp_socket(broadcast: bool = False, timeout: float = 0.5):
    """Gerenciador de contexto para sockets UDP"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        if broadcast:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)
        yield sock
    finally:
        sock.close()

# ============= Sistema de Descoberta =============
class DiscoverySystem:
    """Gerencia descoberta e presença de nós"""
    
    def __init__(self, state: NodeState):
        self.state = state
        self.running = True
    
    def start(self):
        """Inicia threads de descoberta"""
        threads = [
            threading.Thread(target=self._listen_for_discovery, daemon=True),
            threading.Thread(target=self._broadcast_presence, daemon=True),
            threading.Thread(target=self._cleanup_inactive_nodes, daemon=True),
        ]
        for thread in threads:
            thread.start()
        
        # Registra-se
        with self.state.nodes_lock:
            self.state.active_nodes[self.state.node_id] = {
                'address': f"localhost:{self.state.config.port}",
                'last_seen': time.time()
            }
        
        self._run_election()
    
    def _listen_for_discovery(self):
        """Escuta mensagens de descoberta e presença"""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', self.state.config.discovery_port))
            
            while self.running:
                try:
                    data, addr = sock.recvfrom(1024)
                    msg = deserialize_message(data)
                    if not msg:
                        continue
                    
                    if msg['type'] == MessageType.DISCOVERY.value:
                        self._handle_discovery(sock, addr, msg)
                    elif msg['type'] == MessageType.PRESENCE.value:
                        self._handle_presence(addr, msg)
                except Exception as e:
                    logger.error(f"{self.state.get_log_prefix()} Erro no ouvinte de descoberta: {e}")
    
    def _handle_discovery(self, sock, addr, msg):
        """Manipula requisição de descoberta"""
        response = create_message(
            MessageType.DISCOVERY_RESPONSE,
            self.state.node_id,
            self.state.config.port
        )
        sock.sendto(serialize_message(response), addr)
    
    def _handle_presence(self, addr, msg):
        """Manipula mensagem de presença"""
        with self.state.nodes_lock:
            self.state.active_nodes[msg['node_id']] = {
                'address': f"{addr[0]}:{msg['port']}",
                'last_seen': time.time()
            }
        self._run_election()
    
    def _broadcast_presence(self):
        """Transmite presença periodicamente"""
        while self.running:
            time.sleep(self.state.config.discovery_interval)
            with udp_socket(broadcast=True) as sock:
                message = create_message(
                    MessageType.PRESENCE,
                    self.state.node_id,
                    self.state.config.port
                )
                sock.sendto(
                    serialize_message(message),
                    ('<broadcast>', self.state.config.discovery_port)
                )
    
    def _cleanup_inactive_nodes(self):
        """Remove nós inativos"""
        while self.running:
            time.sleep(self.state.config.node_timeout)
            current_time = time.time()
            
            with self.state.nodes_lock:
                to_remove = [
                    nid for nid, info in self.state.active_nodes.items()
                    if current_time - info['last_seen'] > self.state.config.node_timeout
                ]
                for nid in to_remove:
                    del self.state.active_nodes[nid]
                    logger.info(f"{self.state.get_log_prefix()} Nó {nid} removido (timeout)")
            
            self._run_election()
    
    def _run_election(self):
        """Executa eleição Bully via UDP"""
        with self.state.nodes_lock:
            active_copy = dict(self.state.active_nodes)
        
        if not active_copy:
            self.state.leader_id = self.state.node_id
            logger.info(f"{self.state.get_log_prefix()} Eleição: apenas eu, me tornando líder")
            return
        
        # Bully election: highest ID becomes leader
        highest_id = max(active_copy.keys())
        if not self.state.leader_id in active_copy or highest_id > self.state.leader_id:
            self.state.leader_id = highest_id
            logger.info(f"{self.state.get_log_prefix()} Eleição Bully: novo líder é Nó {highest_id}")

# ============= Sincronização de Relógio =============
class ClockManager:
    """Gerencia sincronização de relógio lógico e físico"""
    
    def __init__(self, state: NodeState):
        self.state = state
    
    def update_logical_clock(self, received_clock: int) -> int:
        """Atualiza relógio lógico (Lamport)"""
        self.state.logical_clock = max(self.state.logical_clock, received_clock) + 1
        return self.state.logical_clock
    
    def sync_with_leader(self):
        """Sincroniza relógio físico com o líder"""
        leader = self.state.leader_id
        if leader is None or leader == self.state.node_id:
            return
        
        leader_address = self._get_node_address(leader)
        if not leader_address:
            return
        
        try:
            channel = grpc.insecure_channel(leader_address)
            stub = genetic_pb2_grpc.GeneticNodeStub(channel)
            stub.Heartbeat(
                genetic_pb2.HeartbeatMessage(
                    leader_id=leader,
                    timestamp=time.time()
                )
            )
        except Exception as e:
            logger.error(f"{self.state.get_log_prefix()} Erro na sincronização de relógio: {e}")
    
    def _get_node_address(self, node_id: int) -> Optional[str]:
        with self.state.nodes_lock:
            if node_id in self.state.active_nodes:
                return self.state.active_nodes[node_id]['address']
        return None

# ============= Two-Phase Commit =============
class TwoPhaseCommit:
    """Implementação do protocolo Two-Phase Commit"""
    
    def __init__(self, state: NodeState):
        self.state = state
    
    def send_individual(self, target_id: int, individual: List[float]) -> bool:
        """Envia indivíduo usando 2PC"""
        target_address = self._get_node_address(target_id)
        if not target_address:
            logger.warning(f"{self.state.get_log_prefix()} Nó {target_id} não encontrado")
            return False
        
        self.state.transaction_counter += 1
        transaction_id = self.state.transaction_counter
        
        logger.info(f"{self.state.get_log_prefix()} Iniciando 2PC TX{transaction_id} para Nó {target_id}")
        
        try:
            channel = grpc.insecure_channel(target_address)
            stub = genetic_pb2_grpc.GeneticNodeStub(channel)
            
            # Fase 1: Preparar
            self.state.logical_clock += 1
            prepare_response = stub.Prepare(
                genetic_pb2.PrepareRequest(
                    transaction_id=transaction_id,
                    sender_id=self.state.node_id,
                    individual=genetic_pb2.IndividualMessage(
                        genes=individual,
                        fitness=fitness(individual),
                        sender_id=self.state.node_id,
                        logical_clock=self.state.logical_clock,
                        physical_time=time.time()
                    )
                ),
                timeout=self.state.config.prepare_timeout
            )
            
            if not prepare_response.ready:
                # Fase 2b: Abortar
                logger.warning(f"{self.state.get_log_prefix()} Nó {target_id} rejeitou TX{transaction_id}: {prepare_response.message}")
                stub.Abort(genetic_pb2.AbortRequest(
                    transaction_id=transaction_id,
                    sender_id=self.state.node_id,
                    reason=prepare_response.message
                ))
                return False
            
            # Fase 2a: Confirmar
            logger.info(f"{self.state.get_log_prefix()} Nó {target_id} pronto para confirmar TX{transaction_id}")
            stub.Commit(
                genetic_pb2.CommitRequest(
                    transaction_id=transaction_id,
                    sender_id=self.state.node_id
                ),
                timeout=self.state.config.prepare_timeout
            )
            
            logger.info(f"{self.state.get_log_prefix()} TX{transaction_id} confirmada com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"{self.state.get_log_prefix()} 2PC error TX{transaction_id}: {e}")
            self._attempt_abort(stub, transaction_id, str(e))
            return False
    
    def _attempt_abort(self, stub, transaction_id: int, reason: str):
        """Tenta abortar transação após erro"""
        try:
            stub.Abort(genetic_pb2.AbortRequest(
                transaction_id=transaction_id,
                sender_id=self.state.node_id,
                reason=reason
            ))
        except:
            pass
    
    def _get_node_address(self, node_id: int) -> Optional[str]:
        with self.state.nodes_lock:
            if node_id in self.state.active_nodes:
                return self.state.active_nodes[node_id]['address']
        return None

# ============= Seção Crítica =============
class CriticalSectionManager:
    """Gerencia acesso à seção crítica"""
    
    def __init__(self, state: NodeState):
        self.state = state
    
    @contextmanager
    def acquire(self):
        """Gerenciador de contexto para acesso à seção crítica"""
        if self._request_access():
            try:
                yield True
            finally:
                self._release_access()
        else:
            yield False
    
    def _request_access(self) -> bool:
        """Solicita acesso ao líder"""
        leader = self.state.leader_id
        if not leader or leader == self.state.node_id:
            return True
        
        leader_address = self._get_node_address(leader)
        if not leader_address:
            return False
        
        try:
            channel = grpc.insecure_channel(leader_address)
            stub = genetic_pb2_grpc.GeneticNodeStub(channel)
            response = stub.RequestCriticalSection(
                genetic_pb2.MutexRequest(node_id=self.state.node_id)
            )
            return response.granted
        except Exception as e:
            logger.error(f"{self.state.get_log_prefix()} Erro na solicitação de seção crítica: {e}")
            return False
    
    def _release_access(self):
        """Libera seção crítica"""
        leader = self.state.leader_id
        if not leader or leader == self.state.node_id:
            return
        
        leader_address = self._get_node_address(leader)
        if not leader_address:
            return
        
        try:
            channel = grpc.insecure_channel(leader_address)
            stub = genetic_pb2_grpc.GeneticNodeStub(channel)
            stub.ReleaseCriticalSection(
                genetic_pb2.MutexRequest(node_id=self.state.node_id)
            )
        except Exception as e:
            logger.error(f"{self.state.get_log_prefix()} Erro ao liberar seção crítica: {e}")
    
    def _get_node_address(self, node_id: int) -> Optional[str]:
        with self.state.nodes_lock:
            if node_id in self.state.active_nodes:
                return self.state.active_nodes[node_id]['address']
        return None

# ============= Algoritmo Genético =============
class GeneticAlgorithmManager:
    """Gerencia operações do algoritmo genético"""
    
    def __init__(self, state: NodeState):
        self.state = state
    
    def evolve_generation(self) -> List[float]:
        """Evolui uma geração e retorna o melhor indivíduo"""
        self.state.generation += 1
        
        # Avalia população
        scored = [(ind, fitness(ind)) for ind in self.state.population]
        scored.sort(key=lambda x: x[1], reverse=True)
        
        best = scored[0][0]
        
        logger.info(f"{self.state.get_log_prefix()} Geração {self.state.generation}")
        logger.info(f"{self.state.get_log_prefix()} Melhor fitness: {fitness(best)}")
        logger.info(f"{self.state.get_log_prefix()} Relógio lógico: {self.state.logical_clock}")
        
        # Evolui população
        self.state.population = [
            mutate(best[:]) for _ in range(self.state.config.population_size)
        ]
        
        return best
    
    def add_individual(self, individual: List[float]):
        """Adiciona indivíduo à população"""
        if len(self.state.population) < self.state.config.max_population:
            self.state.population.append(individual)
    
    def should_share(self) -> bool:
        """Verifica se deve compartilhar com outros nós"""
        return (self.state.generation % self.state.config.share_interval == 0)

# ============= Serviço gRPC =============
class GeneticService(genetic_pb2_grpc.GeneticNodeServicer):
    """Implementação do serviço gRPC"""
    
    def __init__(self, state: NodeState, clock_manager: ClockManager):
        self.state = state
        self.clock_manager = clock_manager
    
    def ReceiveIndividual(self, request, context):
        """Recebe indivíduo de outro nó"""
        self.clock_manager.update_logical_clock(request.logical_clock)
        logger.info(f"{self.state.get_log_prefix()} Indivíduo recebido do Nó {request.sender_id}")
        self.state.population.append(list(request.genes))
        return genetic_pb2.Ack(message="Indivíduo recebido")
    
    def RequestCriticalSection(self, request, context):
        """Solicita acesso à seção crítica"""
        with self.state.critical_section_lock:
            if not self.state.resource_busy:
                self.state.resource_busy = True
                logger.info(f"{self.state.get_log_prefix()} SC concedido ao Nó {request.node_id}")
                return genetic_pb2.MutexReply(granted=True)
            return genetic_pb2.MutexReply(granted=False)
    
    def ReleaseCriticalSection(self, request, context):
        """Libera seção crítica"""
        with self.state.critical_section_lock:
            self.state.resource_busy = False
        logger.info(f"{self.state.get_log_prefix()} Nó {request.node_id} liberou a SC")
        return genetic_pb2.Ack(message="Liberada")
    
    def Heartbeat(self, request, context):
        """Sincroniza relógio"""
        self.state.clock_offset = request.timestamp - time.time()
        logger.info(f"{self.state.get_log_prefix()} Relógio físico sincronizado")
        return genetic_pb2.Ack(message="Relógio sincronizado")
    
    def Prepare(self, request, context):
        """Fase de preparação do 2PC"""
        self.clock_manager.update_logical_clock(request.individual.logical_clock)
        
        transaction_id = request.transaction_id
        can_accept = len(self.state.population) < self.state.config.max_population
        
        if can_accept:
            with self.state.transaction_lock:
                self.state.pending_transactions[transaction_id] = {
                    'individual': list(request.individual.genes),
                    'sender_id': request.sender_id,
                    'status': 'prepared'
                }
        
        logger.info(f"{self.state.get_log_prefix()} Prepare TX{transaction_id}: {'Pronto' if can_accept else 'Rejeitado'}")
        return genetic_pb2.PrepareReply(
            ready=can_accept,
            message="OK" if can_accept else "População cheia"
        )
    
    def Commit(self, request, context):
        """Fase de confirmação do 2PC"""
        with self.state.transaction_lock:
            if request.transaction_id in self.state.pending_transactions:
                tx_data = self.state.pending_transactions[request.transaction_id]
                if tx_data['status'] == 'prepared':
                    self.state.population.append(tx_data['individual'])
                    logger.info(f"{self.state.get_log_prefix()} TX{request.transaction_id} confirmada")
                    del self.state.pending_transactions[request.transaction_id]
        return genetic_pb2.Ack(message="Commit realizado")
    
    def Abort(self, request, context):
        """Fase de abortamento do 2PC"""
        with self.state.transaction_lock:
            if request.transaction_id in self.state.pending_transactions:
                del self.state.pending_transactions[request.transaction_id]
                logger.info(f"{self.state.get_log_prefix()} TX{request.transaction_id} abortada")
        return genetic_pb2.Ack(message="Abort realizado")

# ============= Aplicação Principal =============
class DistributedGeneticNode:
    """Classe principal da aplicação"""
    
    def __init__(self, node_id: int):
        self.config = NodeConfig.from_args(node_id)
        self.state = NodeState(node_id, self.config)
        
        # Inicializa subsistemas
        self.discovery = DiscoverySystem(self.state)
        self.clock_manager = ClockManager(self.state)
        self.two_phase_commit = TwoPhaseCommit(self.state)
        self.critical_section = CriticalSectionManager(self.state)
        self.genetic_algorithm = GeneticAlgorithmManager(self.state)
        
        # Inicia servidor gRPC
        self.grpc_server = self._start_grpc_server()
    
    def _start_grpc_server(self):
        """Inicia servidor gRPC"""
        server = grpc.server(futures.ThreadPoolExecutor(
            max_workers=self.config.grpc_threads
        ))
        
        genetic_pb2_grpc.add_GeneticNodeServicer_to_server(
            GeneticService(self.state, self.clock_manager),
            server
        )
        
        server.add_insecure_port(f"[::]:{self.config.port}")
        server.start()
        logger.info(f"{self.state.get_log_prefix()} Servidor gRPC iniciado na porta {self.config.port}")
        return server
    
    def run(self):
        """Loop principal"""
        self.discovery.start()
        
        try:
            while True:
                # Evolui geração
                best = self.genetic_algorithm.evolve_generation()
                
                # Sincroniza relógio
                self.clock_manager.sync_with_leader()
                
                # Compartilha com outros nós se necessário
                if self.genetic_algorithm.should_share():
                    active_nodes = self._get_active_nodes()
                    targets = [n for n in active_nodes if n != self.state.node_id]
                    
                    if targets:
                        with self.critical_section.acquire() as acquired:
                            if acquired:
                                target = random.choice(targets)
                                success = self.two_phase_commit.send_individual(target, best)
                                logger.info(f"{self.state.get_log_prefix()} Compartilhamento via 2PC: {'Sucesso' if success else 'Falha'}")
                
                time.sleep(self.config.generation_interval)
                
        except KeyboardInterrupt:
            self.grpc_server.stop(0)
            logger.info(f"{self.state.get_log_prefix()} Nó parado")
    
    def _get_active_nodes(self) -> List[int]:
        with self.state.nodes_lock:
            return list(self.state.active_nodes.keys())

# ============= Ponto de Entrada =============
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python node.py <node_id>")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    node = DistributedGeneticNode(node_id)
    node.run()
