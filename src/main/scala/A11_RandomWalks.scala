import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable
import java.nio.file.Paths

object A11_RandomWalks {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("RandomWalkClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val outputTxtPath = "A11_simple_random_walk_clusters.txt"

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

    // Step 3: Compute Transition Counts
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

    val edges: RDD[Edge[Double]] = transitionProbabilities.map { case ((from, to), probability) =>
      Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
    }

    val graph = Graph(activitiesRDD, edges)

    // Step 8: Perform Random Walks
    val numWalks = 100
    val walkLength = 2 // 10 before
    val random = new Random()
    val coOccurrenceMap = mutable.Map[(String, String), Int]()
    val vertices = activitiesRDD.collect()
    val vertexMap = vertices.toMap
    val neighborMap = graph.edges.map(e => (e.srcId, (e.dstId, e.attr))).groupByKey().collectAsMap()

    def selectNextVertex(neighbors: Iterable[(VertexId, Double)]): Option[VertexId] = {
      if (neighbors.isEmpty) return None
      val sortedNeighbors = neighbors.toArray.sortBy(_._2)
      val cumulativeProbs = sortedNeighbors.scanLeft(0.0)(_ + _._2).tail
      val randVal = random.nextDouble() * cumulativeProbs.last
      Some(sortedNeighbors(cumulativeProbs.indexWhere(randVal <= _))._1)
    }

    vertices.foreach { case (vertexId, activity) =>
      for (_ <- 1 to numWalks) {
        var currentVertex = vertexId
        var visitedActivities = mutable.Set(activity)

        for (_ <- 1 to walkLength) {
          val neighbors = neighborMap.getOrElse(currentVertex, Iterable())
          selectNextVertex(neighbors) match {
            case Some(v) if vertexMap.contains(v) =>
              val nextActivity = vertexMap(v)
              visitedActivities += nextActivity
              for (a <- visitedActivities; b <- visitedActivities if a != b) {
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

    val finalClusters = mutable.Map[String, mutable.Set[String]]()
    coOccurrenceMap.foreach { case ((activity1, activity2), count) =>
      if (count >= numWalks / 2) {
        val cluster1 = finalClusters.find(_._2.contains(activity1))
        val cluster2 = finalClusters.find(_._2.contains(activity2))
        (cluster1, cluster2) match {
          case (Some((key1, set1)), Some((key2, set2))) =>
            if (key1 != key2) {
              set1 ++= set2
              finalClusters -= key2
            }
          case (Some((_, set1)), None) => set1 += activity2
          case (None, Some((_, set2))) => set2 += activity1
          case (None, None) =>
            finalClusters(activity1) = mutable.Set(activity1, activity2)
        }
      }
    }

    // Save clusters to .txt file
    if (finalClusters.nonEmpty) {
      val writer = new PrintWriter(new File(outputTxtPath))
      finalClusters.zipWithIndex.foreach { case ((_, cluster), index) =>
        writer.println(s"$index:${cluster.mkString(",")}")
      }
      writer.close()
      println(s"Random walk clusters saved to: $outputTxtPath")
    } else {
      println("No clusters were generated.")
    }

    spark.stop()
  }
}
