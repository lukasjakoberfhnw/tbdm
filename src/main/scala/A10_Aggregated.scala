import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable
import java.nio.file.Paths

object A10_Aggregated {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityGraphFinalClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val outputTxtPath = "A10_aggregated.txt"

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

    // Step 6: Dictionary to Track Merging of Activities
    val activityClusters = mutable.Map[String, mutable.Set[String]]()

    // Step 7: Loop Over Probability Thresholds and Merge Clusters
    val probabilityThresholds = 0.05 to 1.0 by 0.05

    for (threshold <- probabilityThresholds) {
      val edges: RDD[Edge[Double]] = transitionProbabilities
        .filter { case ((from, to), probability) => probability >= threshold }
        .map { case ((from, to), probability) =>
          Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
        }

      val graph = Graph(activitiesRDD.mapValues(_.hashCode.toLong), edges)

      val maxIterations = 10
      val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
        (id, attr, newAttr) => math.min(attr, newAttr),
        triplet => Iterator((triplet.dstId, triplet.srcAttr)),
        (a, b) => math.min(a, b)
      )

      val communityAssignments = lpaGraph.vertices
        .join(activitiesRDD)
        .map { case (id, (community, activityName)) => (community, activityName) }
        .groupByKey()
        .mapValues(_.toSet)
        .collect()

      communityAssignments.foreach { case (_, activities) =>
        val existingCluster = activityClusters.find { case (_, members) =>
          members.exists(activities.contains)
        }

        existingCluster match {
          case Some((key, existingSet)) => existingSet ++= activities
          case None => activityClusters(activities.head) = mutable.Set() ++ activities
        }
      }
    }

    // Step 8: Save results in .txt format
    val writer = new PrintWriter(new File(outputTxtPath))
    activityClusters.zipWithIndex.foreach { case ((_, clusterActivities), clusterId) =>
      writer.println(s"$clusterId:${clusterActivities.mkString(",")}")
    }
    writer.close()

    println(s"Final merged cluster results saved to: $outputTxtPath")

    // Stop Spark Session
    spark.stop()
  }
}
