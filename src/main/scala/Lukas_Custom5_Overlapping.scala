import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable
import java.nio.file.Paths
import java.io.PrintWriter


object OverlappingClustering {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("OverlappingClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val aggregatedJsonPath = "final_aggregated_clusters.json"

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

    // Count occurrences of transitions
    val transitionCounts: RDD[((String, String), Int)] = consecutiveRows.map { case (currentRow, nextRow) =>
      val fromActivity = currentRow.getAs[String]("Activity")
      val toActivity = nextRow.getAs[String]("Activity")
      ((fromActivity, toActivity), 1)
    }.reduceByKey(_ + _)

    // Step 4: Compute Total Outgoing Counts
    val totalOutgoingCounts: RDD[(String, Int)] = transitionCounts
      .map { case ((from, _), count) => (from, count) }
      .reduceByKey(_ + _)

    // Step 5: Compute Transition Probabilities
    val transitionProbabilities: RDD[((String, String), Double)] = transitionCounts
      .map { case ((from, to), count) => (from, (to, count)) }
      .join(totalOutgoingCounts)
      .map { case (from, ((to, count), totalCount)) =>
        ((from, to), count.toDouble / totalCount.toDouble)
      }

    // Step 6: Track Cluster Membership Strengths
    val activityClusterMap = mutable.Map[String, mutable.Map[String, Int]]()

    // Step 7: Loop Over Thresholds
    val probabilityThresholds = 0.05 to 1.0 by 0.05

    for (threshold <- probabilityThresholds) {
      // Step 7a: Filter edges based on threshold
      val edges: RDD[Edge[Double]] = transitionProbabilities
        .filter { case ((from, to), probability) => probability >= threshold }
        .map { case ((from, to), probability) =>
          Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
        }

      // Step 7b: Build the Graph
      val graph = Graph(activitiesRDD.mapValues(_.hashCode.toLong), edges)

      // Step 7c: Run LPA
      val maxIterations = 10
      val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
        (id, attr, newAttr) => math.min(attr, newAttr),
        triplet => Iterator((triplet.dstId, triplet.srcAttr)),
        (a, b) => math.min(a, b)
      )

      // Step 7d: Restore Activity Names After LPA
      val communityAssignments = lpaGraph.vertices
        .join(activitiesRDD)
        .map { case (id, (community, activityName)) => (community, activityName) }
        .groupByKey()
        .mapValues(_.toList)
        .collect()

      // Step 7e: Update Activity-Cluster Map
      communityAssignments.foreach { case (community, activities) =>
        activities.foreach { activity =>
          val clusterMap = activityClusterMap.getOrElseUpdate(activity, mutable.Map())
          clusterMap(community.toString) = clusterMap.getOrElse(community.toString, 0) + 1
        }
      }
    }

    // Step 8: Save Overlapping Clusters
    val jsonOverlapping = activityClusterMap.map { case (activity, clusterMap) =>
      val sortedClusters = clusterMap.toSeq.sortBy(-_._2).map { case (cluster, count) =>
        s"""{"cluster": "$cluster", "strength": $count}"""
      }.mkString("[", ",", "]")

      s"""{"activity": "$activity", "clusters": $sortedClusters}"""
    }.mkString("[\n", ",\n", "\n]")

    val writer1 = new PrintWriter(new File(overlappingJsonPath))
    writer1.write(jsonOverlapping)
    writer1.close()
    println(s"Overlapping clusters saved to: $overlappingJsonPath")

    // Step 9: Aggregate Final Clusters Using Strength Thresholds
    val maxStrength = if (activityClusterMap.nonEmpty && activityClusterMap.values.flatMap(_.values).nonEmpty) 
        activityClusterMap.values.flatMap(_.values).max 
    else 0
val aggregatedResults = mutable.ListBuffer[String]()

    for (threshold <- 0 to maxStrength) {
      val filteredClusters = activityClusterMap.map { case (activity, clusters) =>
        val strongClusters = clusters.filter { case (_, strength) => strength >= threshold }.keys.toList
        (activity, strongClusters)
      }.filter(_._2.nonEmpty) // Remove empty entries

      val finalClusters = mutable.Map[String, mutable.Set[String]]()
      filteredClusters.foreach { case (activity, clusters) =>
        clusters.foreach { cluster =>
          finalClusters.getOrElseUpdate(cluster, mutable.Set()) += activity
        }
      }

      val jsonClusters = finalClusters.map { case (cluster, members) =>
        s"""{"cluster": "$cluster", "members": [${members.mkString("\"", "\", \"", "\"")}]}"""
      }.mkString("[\n", ",\n", "\n]")

      aggregatedResults.append(s"""{"threshold": $threshold, "clusters": $jsonClusters}""")
    }

    // Step 10: Save Aggregated Clusters
    val writer2 = new PrintWriter(new File(aggregatedJsonPath))
    writer2.write("[\n" + aggregatedResults.mkString(",\n") + "\n]")
    writer2.close()
    println(s"Final aggregated clusters saved to: $aggregatedJsonPath")

    // Stop Spark Session
    spark.stop()
  }
}
