import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable
import java.nio.file.Paths

// Overlapping attempt

object A9_Overlapping {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("OverlappingClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val resultTxtPath = "A9_overlapping.txt"

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

    // Step 8: Aggregate Final Clusters Using Strength Thresholds
    val maxStrength = if (activityClusterMap.nonEmpty && activityClusterMap.values.flatMap(_.values).nonEmpty)
      activityClusterMap.values.flatMap(_.values).max
    else 0

    val finalClusterMap = mutable.Map[String, mutable.Set[String]]()

    for (threshold <- 0 to maxStrength) {
      val filteredClusters = activityClusterMap.map { case (activity, clusters) =>
        val strongClusters = clusters.filter { case (_, strength) => strength >= threshold }.keys.toList
        (activity, strongClusters)
      }.filter(_._2.nonEmpty) // Remove empty entries

      filteredClusters.foreach { case (activity, clusters) =>
        clusters.foreach { cluster =>
          finalClusterMap.getOrElseUpdate(cluster, mutable.Set()) += activity
        }
      }
    }

    // Step 9: Save Final Clusters to .txt File
    val writer = new PrintWriter(new File(resultTxtPath))
    finalClusterMap.foreach { case (cluster, activities) =>
      writer.write(s"$cluster:${activities.mkString(",")}\n")
    }
    writer.close()
    println(s"Final clusters saved to: $resultTxtPath")

    // Stop Spark Session
    spark.stop()
  }
}
