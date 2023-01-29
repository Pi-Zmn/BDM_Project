# Tests for dht
import math

from dht import *
from random import randint

k = 20

d = DHT(k)

number_of_nodes = 10

# Add nodes
for i in range(number_of_nodes):
    r = randint(0, int(math.pow(2, k)))
    d.join(Node(r))

d.updateAllFingerTables()

print("Chord Ring created with ", d.getNumNodes(), " Nodes")

for i in range(5, 1024, 10):
    d.store(d._startNode, i, "hello" + str(i))

for i in range(5, 200, 10):
    print(d.lookup(d._startNode, i))

