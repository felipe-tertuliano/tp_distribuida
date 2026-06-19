
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import random
import threading
from concurrent import futures
import grpc
import genetic_pb2
import genetic_pb2_grpc
from genetic import create_individual, fitness, mutate

ALL_NODES = {
    1: "localhost:5001",
    2: "localhost:5002",
    3: "localhost:5003"
}

node_id = int(sys.argv[1])
port = int(sys.argv[2])

logical_clock = 0
clock_offset = 0
leader_id = max(ALL_NODES.keys())

critical_section_lock = threading.Lock()
resource_busy = False

population = [create_individual() for _ in range(10)]

class GeneticService(genetic_pb2_grpc.GeneticNodeServicer):

    def ReceiveIndividual(self, request, context):
        global logical_clock

        logical_clock = max(logical_clock, request.logical_clock) + 1

        print(f"[Node {node_id}] Received individual from Node {request.sender_id}")
        print(f"[Node {node_id}] Logical clock updated to {logical_clock}")

        population.append(list(request.genes))

        return genetic_pb2.Ack(message="Individual received")

    def RequestCriticalSection(self, request, context):
        global resource_busy

        with critical_section_lock:
            if not resource_busy:
                resource_busy = True
                print(f"[Node {node_id}] Granted CS access to Node {request.node_id}")
                return genetic_pb2.MutexReply(granted=True)

            return genetic_pb2.MutexReply(granted=False)

    def ReleaseCriticalSection(self, request, context):
        global resource_busy

        with critical_section_lock:
            resource_busy = False

        print(f"[Node {node_id}] Node {request.node_id} released CS")

        return genetic_pb2.Ack(message="Released")

    def Heartbeat(self, request, context):
        global clock_offset

        local_time = time.time()
        clock_offset = request.timestamp - local_time

        print(f"[Node {node_id}] Physical clock synchronized")
        return genetic_pb2.Ack(message="Clock synchronized")

    def Election(self, request, context):
        global leader_id

        if request.node_id < node_id:
            leader_id = node_id

        return genetic_pb2.Ack(message="Election processed")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    genetic_pb2_grpc.add_GeneticNodeServicer_to_server(
        GeneticService(), server
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()

    print(f"[Node {node_id}] gRPC server started on port {port}")

    return server

def synchronize_clocks():
    global clock_offset

    if node_id == leader_id:
        return

    leader_address = ALL_NODES[leader_id]

    try:
        channel = grpc.insecure_channel(leader_address)
        stub = genetic_pb2_grpc.GeneticNodeStub(channel)

        current_time = time.time()

        stub.Heartbeat(
            genetic_pb2.HeartbeatMessage(
                leader_id=leader_id,
                timestamp=current_time
            )
        )

    except Exception as e:
        print(f"[Node {node_id}] Clock sync error: {e}")

def request_critical_section():
    leader_address = ALL_NODES[leader_id]

    channel = grpc.insecure_channel(leader_address)
    stub = genetic_pb2_grpc.GeneticNodeStub(channel)

    response = stub.RequestCriticalSection(
        genetic_pb2.MutexRequest(node_id=node_id)
    )

    return response.granted

def release_critical_section():
    leader_address = ALL_NODES[leader_id]

    channel = grpc.insecure_channel(leader_address)
    stub = genetic_pb2_grpc.GeneticNodeStub(channel)

    stub.ReleaseCriticalSection(
        genetic_pb2.MutexRequest(node_id=node_id)
    )

def send_individual(target_id, individual):
    global logical_clock

    logical_clock += 1

    target_address = ALL_NODES[target_id]

    try:
        channel = grpc.insecure_channel(target_address)
        stub = genetic_pb2_grpc.GeneticNodeStub(channel)

        stub.ReceiveIndividual(
            genetic_pb2.IndividualMessage(
                genes=individual,
                fitness=fitness(individual),
                sender_id=node_id,
                logical_clock=logical_clock,
                physical_time=time.time()
            )
        )

        print(f"[Node {node_id}] Sent individual to Node {target_id}")

    except Exception as e:
        print(f"[Node {node_id}] RPC error: {e}")

server = serve()

print(f"[Node {node_id}] Leader elected: {leader_id}")

generation = 0

try:
    while True:

        generation += 1

        scored = [(ind, fitness(ind)) for ind in population]
        scored.sort(key=lambda x: x[1], reverse=True)

        best = scored[0][0]

        print(f"[Node {node_id}] Generation {generation}")
        print(f"[Node {node_id}] Best fitness: {fitness(best)}")
        print(f"[Node {node_id}] Logical clock: {logical_clock}")

        population[:] = [mutate(best[:]) for _ in range(10)]

        synchronize_clocks()

        if generation % 5 == 0:

            if request_critical_section():

                target = random.choice(
                    [n for n in ALL_NODES if n != node_id]
                )

                send_individual(target, best)

                release_critical_section()

        time.sleep(3)

except KeyboardInterrupt:
    server.stop(0)
