package main.kotlin

class ReadonlyGraph : Graph {
    override val nodeCount: Int get() = nodes.size
    override val edgeCount: Int

    private val nodes: Array<Map<Int, Edge>>
    private val actorTable: Map<Int, String>

    private constructor(nodes: Array<Map<Int, Edge>>, actorTable: Map<Int, String>, edgeCount: Int) {
        this.nodes = nodes
        this.actorTable = actorTable
        this.edgeCount = edgeCount
    }

    fun getActorById(id: Int): String? {
        return actorTable[id]
    }

    fun getShortestPath(fromNode: Int, toNode: Int): List<Segment> {
        val paths = emptyMap<Int, Segment>() as MutableMap
        // Assign to every node a tentative distance value: set it to zero for our initial node and to infinity for all
        // other nodes.
        val distances = DynamicPriorityQueue<Int, Segment>()
        distances.insertOrIncreasePriority(fromNode, 0, Segment(-1, -1, Edge(-1, 0)))

        // Keep a set of visited nodes.  This set starts with just the initial node.

        val visited = emptySet<Int>() as MutableSet
        var curNode = fromNode

        while (visited.size < nodes.size) {
            // For the current node, consider all of its unvisited neighbors and calculate (distance to the current
            // node) + (distance from the current node to neighbor).  If this is less than their current tentative
            // distance, replace it with this new value.

            val curDistance = distances.getPriority(curNode).orElseThrow { RuntimeException("Shouldn't happen") }
            for ((adjacentNode, edge) in nodes[curNode]) {
                if (curNode in visited) {
                    continue
                }

                val currentBestDistance = distances.getPriority(adjacentNode).orElse(Int.MAX_VALUE)
                val testDistance = curDistance + edge.distance
                if (testDistance < currentBestDistance) {
                    distances.insertOrIncreasePriority(adjacentNode, testDistance, Segment(adjacentNode, curNode, edge))
                }
            }

            // When we are done considering all of the neighbors of the current node, mark the current node as visited
            // and remove it from the unvisited set.
            visited.add(curNode)

            // If the destination node has been marked visited, the algorithm has finished.
            if (curNode == toNode) {
                val shortestPath = emptyList<Segment>() as MutableList
                var segment = distances.getNodeMetadata(toNode).orElseThrow { RuntimeException("Shouldn't happen") }
                shortestPath.add(segment)
                var nextNode = segment.otherNode
                while (nextNode != fromNode) {
                    segment = paths[nextNode]
                    shortestPath.add(segment)
                    nextNode = segment.otherNode
                }

                return shortestPath
            }

            // Set the unvisited node marked with the smallest tentative distance as the next "current node" and go
            // back to step 3.
            val segment = distances.getNodeMetadata(curNode).orElseThrow { RuntimeException("Shouldn't happen") }
            paths[curNode] = segment
            curNode = distances.extractMin().orElse(-1)

            // This occurs when there is no path connecting both nodes
            if (curNode == -1) {
                return emptyList()
            }
        }

        throw RuntimeException("Shouldn't get here")
    }
}