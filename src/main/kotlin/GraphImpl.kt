package main.kotlin

import getBytes
import java.io.OutputStream
import java.util.*

class GraphImpl(val _nodeCount: Int) : Graph {

    /**
     * An array of nodes where the array index is the node id (actor id).
     * The resulting map is keyed by its adjacent node ids.
     */
    val nodes: Array<MutableMap<Int, Edge>>

    /**
     * Keeps track of distinct edge references so we don't keep any duplicates.
     */
    private val edges: MutableMap<Edge, MutableSet<Int>> = HashMap()

    init {
        nodes = Array(_nodeCount, { HashMap<Int, Edge>() })
    }

    override val nodeCount: Int get() = _nodeCount
    override val edgeCount: Int get() = edges.size

    fun addEdge(edge: Edge, nodeIds: Array<Int>) {
        if (edge !in edges) {
            edges.put(edge, nodeIds.toHashSet())
        } else {
            val set = edges[edge]!!
            set.addAll(nodeIds)

            makeAdjacent(nodes, nodeIds, edge)
        }
    }

    private fun makeAdjacent(nodes: Array<MutableMap<Int, Edge>>, nodeIds: Array<Int>, edge: Edge) {
        for (i in 0..nodeIds.size) {
            for (j in 0..nodeIds.size) {
                if (i == j) {
                    continue
                }

                val sourceNodeId = nodeIds[i]
                val targetNodeId = nodeIds[j]

                if (targetNodeId in nodes[sourceNodeId]) {
                    if (nodes[sourceNodeId][targetNodeId]!!.distance > edge.distance) {
                        nodes[sourceNodeId][targetNodeId] = edge
                    }
                } else {
                    nodes[sourceNodeId][targetNodeId] = edge
                }
            }
        }
    }

    fun writeToStream(actorTable: Map<Int, String>, outStream: OutputStream) {
        // First byte is a signed 32 bit integer representing the number of actors in the actor table.
        outStream.write(actorTable.size.getBytes())

        for ((key, value) in actorTable) {
            // Each row in the actor table starts with a signed 32-bit integer representing the character length
            // of the actors name, followed by unicode encoded characters of the aforementioned length.
            outStream.write(value.length.getBytes())
            outStream.write(value.getBytes())
        }

        val edges = edges.keys.toTypedArray()

        // The edge table starts with a 32-bit signed integer representing the number of edges in the edge table
        outStream.write(edges.size.getBytes())
        for (i in 0..edges.size) {
            val edge = edges[i]
            // Determine the nodes connected by the edge
            val connectedNodes = this.edges[edge]!!.toTypedArray()

            // The next 4 bytes are a 32-bit signed integer representing the movie id
            outStream.write(edge.movieId.getBytes())

            // The following byte represents the distance (or weight) of the edge
            outStream.write(byteArrayOf(edge.distance))

            // Each edge row starts with a 32-bit signed integer representing the number of nodes connected
            // by the edge.
            outStream.write(connectedNodes.size.getBytes())

            for (connectedNode in connectedNodes) {
                // write each connected node id as a 32-bit signed integer
                outStream.write(connectedNode.getBytes())
            }
        }
    }
}