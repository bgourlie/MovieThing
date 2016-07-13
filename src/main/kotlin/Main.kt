import main.kotlin.Edge
import main.kotlin.GraphImpl
import java.nio.ByteBuffer
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.util.*

private val PRUNE_ON = "Kevin Bacon"
private val IDX_ID = 0
private val IDX_TITLE = 2
private val IDX_YEAR = 3
private val IDX_CAST = 10

fun main(args: Array<String>) {
    if (args.size == 0) {
        println("You must supply a file name")
        return
    }

    val (graph, actorTable, movies) = generateGraph(args[0])

    val graphOutPath = FileSystems.getDefault().getPath("out.bin")

    Files.newOutputStream(graphOutPath).use {
        graph.writeToStream(actorTable, it)
    }

    val graphFileInfo = Files.readAttributes(graphOutPath, BasicFileAttributes::class.java)
    val longestActorName = actorTable.maxBy { it.value.length }
    val mostEdgesForANode = graph.nodes.maxBy { it.values.size }

    println()
    println("Saved graph with ${graph.nodeCount} nodes (highest edges for a node is $mostEdgesForANode), ${graph.edgeCount} edges and ${actorTable.size} actors (max actor name length is $longestActorName) to out.bin (${graphFileInfo.size() / (1024 * 1024)}MB).")


    val movieOutPath = FileSystems.getDefault().getPath("movies.bin")
    Files.newOutputStream(movieOutPath).use {
        writer ->

        for ((id, title) in movies) {
            writer.write(id.getBytes())
            writer.write(title.length.getBytes())
            writer.write(title.getBytes())
        }
    }

    val moviesFileInfo = Files.readAttributes(graphOutPath, BasicFileAttributes::class.java)
    println()
    println("Saved movies table ${moviesFileInfo.size() / (1024 * 1024)}MB")
//
//    Console.WriteLine("Creating readonly graph from file (smoke test)");
//
//    using (var fileStream = File.OpenRead("out.bin"))
//    {
//        var readonlyGraph = ReadonlyGraph.NewFromStream(fileStream);
//        GC.Collect();
//        var memoryUsage = GetMemoryUsageInMegaBytes();
//        Console.WriteLine("Readonly graph created with {0} nodes and {1} edges.  Memory used by process is {2}..", readonlyGraph.NodeCount, readonlyGraph.EdgeCount, memoryUsage);
//    }
//
//    Console.WriteLine("Writing human readable actor table to actors.txt");
//    using (var fileStream = File.OpenWrite("actors.txt"))
//    {
//        using (var streamWriter = new StreamWriter(fileStream))
//        {
//            foreach (var actor in actorTable)
//            {
//                streamWriter.WriteLine("{0}: {1}", actor.Key.ToString("D8"), actor.Value);
//            }
//        }
//    }
//    Console.WriteLine("Press any key to exit");
//    Console.Read();
}

fun prune(graph: GraphImpl, sourceNodeId: Int): List<Pair<Int, Map<Int, Edge>>> {
    val visited = HashSet<Int>()
    val queue: Queue<Int> = LinkedList<Int>()
    visited.add(sourceNodeId)
    var curNode = sourceNodeId

    do {
        val edges = graph.nodes[curNode]
        for (key in edges.keys) {
            if (key in visited) {
                continue
            }

            visited.add(key)
            queue.add(key)
        }

        curNode = if (queue.size == 0) -1 else queue.remove()
    } while (curNode > -1)

    return visited.map { Pair(it, graph.nodes[it] as Map<Int, Edge>) }
}

private fun generateGraph(movieDump: String): Triple<GraphImpl, Map<Int, String>, Set<MovieEntry>> {
    val path = FileSystems.getDefault().getPath(movieDump)
    Files.newBufferedReader(path).use {
        reader ->
        run {
            val movies = mutableListOf<MovieEntry>()
            val actorsByName = mutableMapOf<String, Int>()
            val actorsById = mutableMapOf<Int, String>()
            val actorsMovies = mutableMapOf<String, MutableList<MovieEntry>>()
            var processed = 0
            var skippedMovies = 0
            var line: String? = null

            println("Processing Movie Dump")

            while ({ line = reader.readLine(); line }() != null) {
                val parts = line!!.split('\t')

                val title = parts[IDX_TITLE]

                val id = try {
                    parts[IDX_ID].toInt()
                } catch (e: NumberFormatException) {
                    skippedMovies++
                    continue
                }

                val year = try {
                    parts[IDX_YEAR].toShort()
                } catch (e: NumberFormatException) {
                    skippedMovies++
                    continue
                }

                val cast = getCast(parts[IDX_CAST])
                if (cast.isEmpty()) {
                    skippedMovies++
                    continue
                }

                val movieEntry = MovieEntry(id, title, year, cast)

                movies.add(movieEntry)

                for (member in cast) {
                    if (member !in actorsByName) {
                        actorsByName.put(member, -1)
                    }

                    // We index movies by actor so that we can prune movies later based on
                    // whether or not the cast is reachable within our graph
                    actorsMovies.getOrPut(member, { mutableListOf(movieEntry) }).add(movieEntry)
                }

                if ((++processed) % 10000 == 0) {
                    print(".")
                }
            }

            println()
            print("Processed ${movies.size}. Skipped $skippedMovies")

            // Create a lookup table so we can determine an actor's id by their name.
            val arrActors = actorsByName.keys.toTypedArray()
            for (actorId in 0..arrActors.size) {
                val name = arrActors[actorId]
                actorsByName[name] = actorId
                actorsById.put(actorId, name)
            }

            println()
            println("Generating graph with ${arrActors.size} nodes")

            val graph = GraphImpl(arrActors.size)

            for (i in 0..movies.size) {
                val entry = movies[i]
                val actorIds = Array(entry.cast.size, { j -> actorsByName[entry.cast[j]]!! })
                val edge = Edge(entry.id, (255 - (2015 - entry.year)).toByte())
                graph.addEdge(edge, actorIds)

                if (i % 10000 == 0) {
                    print(".")
                }
            }

            val pruned = prune(graph, actorsByName[PRUNE_ON]!!)
            println("${pruned.size} nodes left after pruning")

            // original Id is key, new id is value
            val actorTranslationTableByOrigId = HashMap<Int, Int>(pruned.size)
            val actorTranslationTableByNewId = HashMap<Int, Int>(pruned.size)

            for (i in 0..pruned.size) {
                actorTranslationTableByOrigId.put(pruned[i].first, i)
                actorTranslationTableByNewId.put(i, pruned[i].first)
            }

            println("Creating new pruned graph")
            val prunedGraph = GraphImpl(pruned.size)
            val newActorsById = HashMap<Int, String>()
            val prunedActors = HashSet<String>()

            for (i in 0..pruned.size) {
                val actorId = actorsById[actorTranslationTableByNewId[i]]
                newActorsById.put(i, actorId!!)
                prunedActors.add(actorId)

                for ((key, value) in pruned[i].second) {
                    val edge = Edge(value.movieId, value.distance)
                    prunedGraph.addEdge(edge, arrayOf(i, actorTranslationTableByOrigId[key]!!))
                }

                if (i % 10000 == 0) {
                    println(".")
                }
            }

            val prunedMovies = HashSet<MovieEntry>()
            for (actor in prunedActors) {
                for (movie in actorsMovies[actor]!!) {
                    prunedMovies.add(movie)
                }
            }

            return Triple(prunedGraph, newActorsById, prunedMovies)
        }
    }
}

private fun getCast(castValue: String): Array<String> {
    val cast = castValue.split(", ").filter { it.isNotEmpty() }

    // In order to link two actors to each other, we need at least two actors cast in a movie!
    // TODO: Document all conditions in which we return an empty list
    if (cast.size <= 1 || cast[0].equals("N/A") || cast.any { !it.contains(" ") }) {
        // TODO: logging statements for when we return empty for reasons above
        return emptyArray()
    }

    return cast.toTypedArray()
}


fun Int.getBytes() = ByteBuffer.allocate(4).putInt(this).array()!!

fun String.getBytes() = this.toByteArray(Charsets.UTF_16)

private data class MovieEntry(val id: Int, val title: String, val year: Short, val cast: Array<String>) {
    override fun hashCode(): Int {
        return id
    }

    override fun equals(other: Any?): Boolean {
        return id.equals(other)
    }
}
