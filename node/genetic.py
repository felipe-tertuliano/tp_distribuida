
import random

ITEMS = [(10,5),(20,4),(30,6),(40,3),(25,5),(35,6),(50,7)]
MAX_WEIGHT = 20

def create_individual():
    return [random.randint(0,1) for _ in range(len(ITEMS))]

def fitness(individual):
    value = 0
    weight = 0

    for gene, item in zip(individual, ITEMS):
        if gene:
            value += item[0]
            weight += item[1]

    if weight > MAX_WEIGHT:
        return 0

    return value

def mutate(individual):
    idx = random.randint(0, len(individual)-1)
    individual[idx] = 1 - individual[idx]
    return individual
