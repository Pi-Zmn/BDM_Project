# A Distributed Hash Table implementation
import json
from termcolor import colored

class Node:
    def __init__(self, ID, nxt = None, prev = None):
        self.ID = ID
        self.data = dict()
        self.prev = prev
        self.fingerTable = [nxt]

    # Update the finger table of this node when necessary
    def updateFingerTable(self, dht, k):
        del self.fingerTable[1:]
        for i in range(1, k):
            self.fingerTable.append(dht.findNode(dht._startNode, self.ID + 2 ** i))

    def toString(self):
        print('###################')
        print(colored('Node ' + str(self.ID), 'blue'))
        print('holds ', len(self.data), ' values...')
        print(json.dumps(self.data, sort_keys = True, indent = 4))

    def dataDistribution(self, total_data, num_of_nodes):
        print(colored('Node ' + str(self.ID), 'blue'), "holds\t",
              colored(str(len(self.data) if self.data else 0), "cyan"), "values",
              "\t| optimal distribution ", colored(str(total_data/num_of_nodes), "green"))


class DHT:
    # The total number of IDs available in the DHT is 2 ** k
    def __init__(self, k, logging):
        self._logging = logging
        self._insert_hash_collisions = 0

        self._k = k
        self._size = 2 ** k
        self._startNode = Node(0, k)
        self._startNode.fingerTable[0] = self._startNode
        self._startNode.prev = self._startNode
        self._startNode.updateFingerTable(self, k)

    # Hash function used to get the ID
    def getHashId(self, key):
        return key % self._size

    # Get distance between to IDs
    def distance(self, n1, n2):
        if n1 == n2:
            return 0
        if n1 < n2:
            return n2 - n1
        return self._size - n1 + n2

    # Get number of nodes in the system
    def getNumNodes(self):
        if self._startNode == None:
            return 0
        node = self._startNode
        n = 1
        while node.fingerTable[0] != self._startNode:
            n = n + 1
            node = node.fingerTable[0]
        return n

    # Find the node responsible for the key
    def findNode(self, start, key):
        hashId = self.getHashId(key)
        curr = start
        numJumps = 0
        while True:
            if curr.ID == hashId:
                if self._logging:
                    print(colored("number of jumps: " + str(numJumps), "green"))
                return curr
            if self.distance(curr.ID, hashId) <= self.distance(curr.fingerTable[0].ID, hashId):
                if self._logging:
                    print(colored("number of jumps: " + str(numJumps), "green"))
                return curr.fingerTable[0]
            tabSize = len(curr.fingerTable)
            i = 0
            nextNode = curr.fingerTable[-1]
            while i < tabSize - 1:
                if self.distance(curr.fingerTable[i].ID, hashId) < self.distance(curr.fingerTable[i + 1].ID, hashId):
                    nextNode = curr.fingerTable[i]
                i = i + 1
            curr = nextNode
            numJumps += 1


    # Look up a key in the DHT
    def lookup(self, start, key):
        nodeForKey = self.findNode(start, key)
        if key in nodeForKey.data:
            if self._logging:
                print("The key is in node: ", nodeForKey.ID)
            return nodeForKey.data[key]
        return None

    # Store a key-value pair in the DHT
    def store(self, start, key, value, isInsert):
        nodeForKey = self.findNode(start, key)
        if isInsert and key in nodeForKey.data:
            self._insert_hash_collisions += 1
            if self._logging:
                print(colored("Hash collision on insert", "red"))
        nodeForKey.data[key] = value
        # TODO: ADD REPLICATION

    # When new node joins the system
    def join(self, newNode):
        # Find the node before which the new node should be inserted
        origNode = self.findNode(self._startNode, newNode.ID)

        if self._logging:
            print(origNode.ID, "  ", newNode.ID)
        # If there is a node with the same id, decline the join request for now
        if origNode.ID == newNode.ID:
            if self._logging:
                print(colored("There is already a node with the same id!", "red"))
            return False

        # Copy the key-value pairs that will belong to the new node after
        # the node is inserted in the system
        for key in origNode.data:
            hashId = self.getHashId(key)
            if self.distance(hashId, newNode.ID) < self.distance(hashId, origNode.ID):
                newNode.data[key] = origNode.data[key]

        # Update the prev and next pointers
        prevNode = origNode.prev
        newNode.fingerTable[0] = origNode
        newNode.prev = prevNode
        origNode.prev = newNode
        prevNode.fingerTable[0] = newNode

        # Set up finger table of the new node
        newNode.updateFingerTable(self, self._k)

        # Delete keys that have been moved to new node
        for key in list(origNode.data.keys()):
            hashId = self.getHashId(key)
            if self.distance(hashId, newNode.ID) < self.distance(hashId, origNode.ID):
                del origNode.data[key]

        return True


    def leave(self, node):
        # Copy all its key-value pairs to its successor in the system
        for k, v in node.data.items():
            node.fingerTable[0].data[k] = v
        # If this node is the only node in the system.
        if node.fingerTable[0] == node:
            self._startNode = None
        else:
            node.prev.fingerTable[0] = node.fingerTable[0]
            node.fingerTable[0] = prev = node.prev
            # If this deleted node was an entry point to the system, we
            # need to choose another entry point. Simply choose its successor
            if self._startNode == node:
                self._startNode = node.fingerTable[0]

    def updateAllFingerTables(self):
        self._startNode.updateFingerTable(self, self._k)
        curr = self._startNode.fingerTable[0]
        while curr != self._startNode:
            curr.updateFingerTable(self, self._k)
            curr = curr.fingerTable[0]

    def getSizeOfDataSet(self):
        node = self._startNode
        size = len(node.data)
        while node.fingerTable[0] != self._startNode:
            node = node.fingerTable[0]
            size += len(node.data)
        return size
    def getMetaData(self):
        numNodes = self.getNumNodes()
        dataSize = self.getSizeOfDataSet()
        print(colored(str(numNodes), "green"), "\tNodes")
        print(colored(str(dataSize), "blue"), "\tKey-Value Pairs")
        print(colored(str(self._size), "cyan"), "\tSize")
        print(colored(str(self._insert_hash_collisions), "red"), "\tHash Collisions occurred during data insertion")

        node = self._startNode
        node.dataDistribution(dataSize, numNodes)
        while node.fingerTable[0] != self._startNode:
            node = node.fingerTable[0]
            node.dataDistribution(dataSize, numNodes)