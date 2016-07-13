package main.kotlin

import java.util.*

class DynamicPriorityQueue<TNode, TMetadata> {
    private val nodes = emptyList<TNode>() as MutableList
    private val keys = emptyList<Pair<Int, TMetadata>>() as MutableList
    private val indices = mapOf<TNode, Int>() as MutableMap

    private var ubound = -1

    private fun insert(item: TNode, key: Int, metaData: TMetadata) {
        val addedAt: Int
        if (ubound == nodes.size - 1) {
            nodes.add(item)
            keys.add(Pair(key, metaData))
            addedAt = 0
        } else {
            nodes[ubound] = item
            keys[ubound] = Pair(key, metaData)
            addedAt = ubound
        }

        indices.put(item, addedAt)
        balanceUp(++ubound)
    }

    fun peekMin(): TNode = nodes[0]

    fun getPriority(node: TNode): Optional<Int> {
        val index = indices[node] ?: return Optional.empty()
        return Optional.of(keys[index].first)
    }

    fun getNodeMetadata(node: TNode): Optional<TMetadata> {
        // keys[indices[node]!!].second
        val index = indices[node] ?: return Optional.empty()
        return Optional.of(keys[index].second)
    }

    fun extractMin(): Optional<TNode> {
        if (ubound == -1) {
            return Optional.empty()
        }

        val ret = nodes[0]
        nodes[0] = nodes[ubound]
        keys[0] = keys[ubound]
        ubound -= 1
        balanceDown(0)
        return Optional.of(ret)
    }

    private fun increasePriority(node: TNode, newPriority: Int, metadata: TMetadata) {
        val index = indices[node]!!
        val curPriority = keys[index]

        if (curPriority.first == newPriority || newPriority > curPriority.first) {
            return
        }

        keys[index] = Pair(newPriority, metadata)
        balanceUp(index)
    }

    fun insertOrIncreasePriority(node: TNode, priority: Int, metadata: TMetadata) {
        if (indices.containsKey(node)) {
            increasePriority(node, priority, metadata)
        } else {
            insert(node, priority, metadata)
        }
    }

    private tailrec fun balanceDown(index: Int) {
        val leftChild = index * 2 + 1
        val rightChild = leftChild + 1

        if (leftChild > ubound) {
            return
        }

        // determine which child node is smaller
        val indexToSwap = if (rightChild > ubound) {
            leftChild
        } else {
            if (keys[leftChild].first < keys[rightChild].first) {
                leftChild
            } else {
                rightChild
            }
        }

        // compare the current node with the smaller of the two child nodes
        if (keys[index].first > keys[indexToSwap].first) {
            // if the current node is bigger than the smaller of the two children,
            // then continue to push that node down by swapping the two nodes
            swapNodes(index, indexToSwap)
            balanceDown(indexToSwap)
        }
    }

    private tailrec fun balanceUp(index: Int) {
        if (index == 0) {
            return
        }

        val parentIndex = index / 2

        if (keys[index].first < keys[parentIndex].first) {
            swapNodes(index, parentIndex)
            balanceUp(parentIndex)
        }
    }

    private fun swapNodes(i: Int, j: Int) {
        val tmpNode = nodes[j]
        nodes[j] = nodes[i]
        nodes[i] = tmpNode

        val tmpKey = keys[j]
        keys[j] = keys[i]
        keys[i] = tmpKey

        indices[nodes[i]] = i
        indices[nodes[j]] = j
    }
}