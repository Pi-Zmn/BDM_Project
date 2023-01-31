import math
import sys, getopt
import time

from PChordLib.dht import *
from random import randint

# Add node
def addNode(d, iD, nodes):
    did_create_node = False
    while not did_create_node:
        node = Node(iD)
        did_create_node = d.join(node)
        if did_create_node:
            nodes.append(node)

# Add all Nodes to Ring
def insertNodes(d, s, k, distribution, nodes):
    for i in range(s - 1): # subtract one because DHT has 1 (start_node) on initialization
        nodeID = None
        if distribution == "random":
            nodeID = randint(0, int(math.pow(2, k)))
        elif distribution == "equal":
            equal_distance = math.ceil(d._size / s)
            nodeID = equal_distance + i * equal_distance
        elif distribution == "hash":
            # TODO: Implement better hash function
            nodeID = d.getHashId(hash("Node-" + str(i)))
        addNode(d, nodeID, nodes)
    d.updateAllFingerTables()

# Add Key-Value Pais
def insertData(d, e, k, n, insertStrategy, index):
    for i in range(e):
        key = None
        if insertStrategy == "random":
            key = randint(0, int(math.pow(2, k) - 1))
        elif insertStrategy == "equal":
            equal_distance = math.ceil(d._size / e)
            key = equal_distance + i * equal_distance
        elif insertStrategy == "hash":
            value = "Value-" + str(i)
            key = d.getHashId(hash(value))
        d.store(d._startNode, key, "Value for Key " + str(key), True)
        index[i] = key

def calculateHashCollisionEstimate(d, e):
    collison_prob = math.ceil((e * (e - 1)) / (2 * d._size))
    print("Hash Collision probability: C = M * (M-1) / 2T = ", colored(str(collison_prob), "red"))

# Create a Bar plot with horizontal threshold
def barPlot(x, y, threshold, title):
    plt.bar(x, y)
    plt.axhline(y=threshold, xmin=0, xmax=1, color='red', linestyle='--')
    plt.xlabel("Nodes")
    plt.ylabel("Data Sets")
    plt.suptitle(title)
    filePath = 'assets/' + title + "-" + time.strftime("%Y%m%d-%H%M%S") + '.png'
    plt.savefig(filePath, dpi=300, bbox_inches='tight')
    plt.show()
def main(argv):
    print(argv)
    s = int(argv[0]) #s = 10
    n = int(argv[1]) #n = 3
    e = int(argv[2]) #e = 10000
    hasLogging = argv[5].lower() == "true"
    # calculate k to avoid hash collisions with given e
    k = math.ceil(math.log2(e)) * 2 # k = math.ceil(math.log2(e)) (?)
    # Create DHT Ring
    d = DHT(k, hasLogging)
    calculateHashCollisionEstimate(d, e)

    nodes = [d._startNode]  # holds all created Node Objects
    index = dict()          # holds all IDs of created Data

    distribution = argv[3] # "equal" | "random" | "hash"
    insertStrategy = argv[4] # "equal" | "random" | "hash"

    insertNodes(d, s, k, distribution, nodes)
    insertData(d, e, k, n, insertStrategy, index)

    distribution_tupel = d.getDataDistribution()
    barPlot(distribution_tupel[0], distribution_tupel[1], distribution_tupel[2],
            (distribution + "_" + insertStrategy))

    #for i in range(5, 200, 10):
    #    print(d.lookup(d._startNode, i))

    #for n in nodes:
        #n.toString()
    #    n.dataDistribution(e, s)

if __name__ == "__main__":
   main(sys.argv[1:])