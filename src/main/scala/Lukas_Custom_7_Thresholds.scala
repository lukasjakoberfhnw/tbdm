import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable

object RandomWalkThresholds {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("RandomWalkClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = "/home/lukas/temp/sorted_logfile.csv"
    val outputFolder = "random_walk_clusters_thresholds"

    // Ensure output folder exists
    new File(outputFolder).mkdirs()

    // Step 1: Load CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Step 2: Extract Distinct Activities (Vertices)
    val activitiesRDD: RDD[(VertexId, String)] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, activity)
    }).distinct()

    val activityIdToName = activitiesRDD.collectAsMap()

    // Step 3: Compute Transition Probabilities
    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) })
      .values

    val transitionCounts: RDD[((String, String), Int)] = consecutiveRows.map { case (currentRow, nextRow) =>
      val fromActivity = currentRow.getAs[String]("Activity")
      val toActivity = nextRow.getAs[String]("Activity")
      ((fromActivity, toActivity), 1)
    }.reduceByKey(_ + _)

    val totalOutgoingCounts: RDD[(String, Int)] = transitionCounts
      .map { case ((from, _), count) => (from, count) }
      .reduceByKey(_ + _)

    val transitionProbabilities: RDD[((String, String), Double)] = transitionCounts
      .map { case ((from, to), count) => (from, (to, count)) }
      .join(totalOutgoingCounts)
      .map { case (from, ((to, count), totalCount)) =>
        ((from, to), count.toDouble / totalCount.toDouble)
      }

    // Step 4: Convert Activity Names to Long IDs for GraphX
    val edges: RDD[Edge[Double]] = transitionProbabilities.map { case ((from, to), probability) =>
      Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
    }

    val graph = Graph(activitiesRDD.mapValues(_.hashCode.toLong), edges)

    // Step 5: Perform Random Walks
    val numWalks = 1000
    val walkLength = 10
    val random = new Random()
    val coOccurrenceMap = mutable.Map[(Long, Long), Int]()

    val vertices = activitiesRDD.collect()
    val neighborMap = graph.edges.map(e => (e.srcId, (e.dstId, e.attr))).groupByKey().collectAsMap()

    def selectNextVertex(neighbors: Iterable[(VertexId, Double)]): Option[VertexId] = {
      if (neighbors.isEmpty) return None
      val sortedNeighbors = neighbors.toArray.sortBy(_._2)
      val cumulativeProbs = sortedNeighbors.scanLeft(0.0)(_ + _._2).tail
      val randVal = random.nextDouble() * cumulativeProbs.last
      Some(sortedNeighbors(cumulativeProbs.indexWhere(randVal <= _))._1)
    }

    vertices.foreach { case (vertexId, _) =>
      for (_ <- 1 to numWalks) {
        var currentVertex = vertexId
        var visitedVertices = mutable.Set(vertexId)

        for (_ <- 1 to walkLength) {
          val neighbors = neighborMap.getOrElse(currentVertex, Iterable())

          selectNextVertex(neighbors) match {
            case Some(v) =>
              visitedVertices += v
              for (a <- visitedVertices; b <- visitedVertices if a != b) {
                val key = if (a < b) (a, b) else (b, a)
                coOccurrenceMap(key) = coOccurrenceMap.getOrElse(key, 0) + 1
              }
              currentVertex = v
            case _ =>
              currentVertex = vertices(random.nextInt(vertices.length))._1
          }
        }
      }
    }

    // Step 6: Expand Threshold Dynamically and Save Results
    val maxWalks = numWalks.toDouble
    val thresholdSteps = 0.5 to 1.0 by 0.05

    var finalClusters = mutable.Map[Long, mutable.Set[Long]]()

    for (threshold <- thresholdSteps) {
      val minCoOccurrence = (maxWalks * threshold).toInt
      val strongConnections = coOccurrenceMap.filter { case (_, count) => count >= minCoOccurrence }

      val clusterEdges: RDD[Edge[Double]] = sc.parallelize(
        strongConnections.map { case ((v1, v2), count) =>
          Edge(v1, v2, count.toDouble)
        }.toSeq
      )

      val clusterGraph = Graph(activitiesRDD.mapValues(_.hashCode.toLong), clusterEdges)

      // Apply LPA for Clustering at Each Threshold
      val maxIterations = 10
      val lpaGraph = clusterGraph.pregel(Long.MaxValue, maxIterations)(
        (id, attr, newAttr) => math.min(attr, newAttr),
        triplet => Iterator((triplet.dstId, triplet.srcAttr)),
        (a, b) => math.min(a, b)
      )

      // Collect Clusters
      val clusters = lpaGraph.vertices
        .map { case (id, clusterId) => (clusterId, id) }
        .groupByKey()
        .mapValues(_.toSet)
        .collect()

      // Save Clusters for Current Threshold
      val jsonThresholdClusters = clusters.map { case (_, cluster) =>
        val namedCluster = cluster.map(id => activityIdToName.getOrElse(id, "Unknown"))
        s"""{"members": [${namedCluster.mkString("\"", "\", \"", "\"")}]}"""
      }.mkString("[\n", ",\n", "\n]")

      val thresholdFilename = f"$outputFolder/clusters_threshold_${(threshold * 100).toInt}.json"
      val writer = new PrintWriter(new File(thresholdFilename))
      writer.write(jsonThresholdClusters)
      writer.close()

      println(s"Saved clusters for threshold ${(threshold * 100).toInt}% to: $thresholdFilename")

      // Merge into final stable clusters
      clusters.foreach { case (clusterId, members) =>
        val existingCluster = finalClusters.find(_._2.intersect(members).nonEmpty)

        existingCluster match {
          case Some((key, set)) => set ++= members
          case None             => finalClusters(clusterId) = mutable.Set() ++ members
        }
      }
    }

    // Stop Spark Session
    spark.stop()
  }
}
