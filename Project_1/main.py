import math
import sys, getopt
import time

import matplotlib.pyplot as plt

from PChordLib.dht import *
from random import randint
import hashlib

# Returns HASH value as INT of string input
def intHash(value):
    sha1 = hashlib.sha256()
    sha1.update(value.encode('utf-8'))
    return int(sha1.hexdigest(), 16)

# Add node
def addNode(d, iD, nodes):
    did_create_node = False
    while not did_create_node:
        node = Node(iD)
        did_create_node = d.join(node)
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
            nodeID = d.getHashId(intHash(str(i)))
        addNode(d, nodeID, nodes)
    d.updateAllFingerTables()

# Add Key-Value Pais
def insertData(d, e, k, insertStrategy, index):
    for i in range(e):
        key = None
        if insertStrategy == "random":
            key = randint(0, int(math.pow(2, k) - 1))
        elif insertStrategy == "equal":
            equal_distance = math.ceil(d._size / e)
            key = equal_distance + i * equal_distance
        elif insertStrategy == "hash":
            key = d.getHashId(intHash(str(i)))
        value = "Value for Key " + str(i)
        d.store(d._startNode, key, value, True)
        #index[i] = key
        index[i] = value

def calculateHashCollisionEstimate(d, e):
    collison_prob = math.ceil((e * (e - 1)) / (2 * d._size))
    print("Hash Collision probability: C = M * (M-1) / 2T = ", colored(str(collison_prob), "red"))

# Create a Bar plot with horizontal threshold
def barPlot(x, y, threshold, title, savePNG):
    plt.bar(x, y)
    plt.axhline(y=threshold, xmin=0, xmax=1, color='red', linestyle='--')
    plt.xlabel("Nodes")
    plt.ylabel("Data Sets")
    plt.suptitle(title)
    if savePNG:
        filePath = 'assets/' + title + "-" + time.strftime("%Y%m%d-%H%M%S") + '.png'
        plt.savefig(filePath, dpi=300, bbox_inches='tight')
    #plt.show()
    plt.clf()

def test_hashAllocation(d, index):
    for k in index:
        found = d.lookup(d._startNode, d.getHashId(intHash(str(k))))
        if not found == index[k]:
            print(colored("Hash Allocation Failed!", "red"))
            print(found, index[k])

def write_workload(w, d, k, index):
    for rW in range(1, w):
        # check if ID already in use
        iD = 1
        while iD in index:
            iD = randint(0, int(math.pow(2, k) - 1))
        key = d.getHashId(intHash(str(iD)))
        value = "Random Value-" + str(rW)
        d.store(d._startNode, key, value, True)
        if (rW % 500000 == 0):
            print("writing random data...")

def main(argv):
    print(argv)
    # TODO: if argv emtpty use default values
    s = int(argv[0]) #s = 10
    n = int(argv[1]) #n = 3
    e = int(argv[2]) #e = 10000
    w = 1000000
    hasLogging = argv[5].lower() == "true"
    # calculate k to avoid hash collisions with given e
    #k = math.ceil(math.log2((e * (e-1))/2))
    # calculate k to avoid hash collisions for 4.000.000 unique values
    k = math.ceil(math.log2((4000000 * (4000000-1))/2))
    # Create DHT Ring
    d = DHT(k, n, hasLogging)
    calculateHashCollisionEstimate(d, e)

    nodes = [d._startNode]  # holds all created Node Objects
    index = dict()          # holds all IDs with Keys of created Data

    distribution = argv[3] # "equal" | "random" | "hash"
    insertStrategy = argv[4] # "equal" | "random" | "hash"

    insertNodes(d, s, k, distribution, nodes)
    insertData(d, e, k, insertStrategy, index)

    distribution_tupel = d.getDataDistribution()
    barPlot(distribution_tupel[0], distribution_tupel[1], distribution_tupel[2],
           (distribution + "_" + insertStrategy), False)

    #if distribution == "hash" and insertStrategy == "hash":
    #    test_hashAllocation(d, index)

    textFile1 = open('assets/' + "writeDistribution" + "-" + time.strftime("%Y%m%d-%H%M%S") + ".txt", "w")
    textFile2 = open('assets/' + "hashCollisions" + "-" + time.strftime("%Y%m%d-%H%M%S") + ".txt", "w")

    # write to a random Key
    write_workload(w, d, k, index)
    for n in nodes:
        textFile1.write("Node\t" + str(n.ID) + ":\t" + str(n.numberOfWrites) + " write Operations\r\n")
    textFile1.write("--------------------------------------------------------------------\r\n")

    textFile2.write("Cluster with " + str(d.getNumNodes()) + " had "
                    + str(d._insert_hash_collisions) + " for w=" + str(w) + "\r\n")
    textFile2.write("------------------------\r\n")

    distribution_tupel = d.getDataDistribution()
    barPlot(distribution_tupel[0], distribution_tupel[1], distribution_tupel[2],
            (distribution + "_" + insertStrategy), False)

    while s < 30:
        # add 5 servers 
        for i in range(0,5):
            s += 1
            nodeID = d.getHashId(intHash(str(s)))
            addNode(d, nodeID, nodes)
        d.updateAllFingerTables()
        # do workload
        write_workload(w, d, k, index)
        for n in nodes:
            textFile1.write("Node\t" + str(n.ID) + ":\t" + str(n.numberOfWrites) + " write Operations\r\n")
        textFile1.write("--------------------------------------------------------------------\r\n")

        textFile2.write("Cluster with " + str(d.getNumNodes()) + " had "
                        + str(d._insert_hash_collisions) + " for w=" + str(w) + "\r\n")
        textFile2.write("------------------------\r\n")

        # get distribution and create plot
        distribution_tupel = d.getDataDistribution()
        barPlot(distribution_tupel[0], distribution_tupel[1], distribution_tupel[2],
                (distribution + "_" + insertStrategy + "_" + str(s) + "Nodes"), True)

    #for i in range(5, 200, 10):
    #    print(d.lookup(d._startNode, i))

    #for n in nodes:
     #   n.toString()
    #    n.dataDistribution(e, s)
        #print(colored("Node " + str(n.ID), "green"), " has " + str(len(n.fingerTable)) + " Fingertable entrys")
        #for f in n.fingerTable:
        #    print(colored("\t" + str(f.ID), "blue"))

if __name__ == "__main__":
   main(sys.argv[1:])