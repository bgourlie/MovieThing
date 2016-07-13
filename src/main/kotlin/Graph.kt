package main.kotlin

interface Graph {
    val nodeCount: Int
    val edgeCount: Int
}

data class Edge(val movieId: Int, val distance: Byte)

data class Segment(val node: Int, val otherNode: Int, val edge: Edge)