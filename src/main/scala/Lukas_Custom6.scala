import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable

object Lukas_Custom6 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityGraphFinalClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = "/home/lukas/temp/sorted_logfile.csv"
    val outputJsonPath = "custom6.json"

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
      // Step 7a: Filter edges based on threshold
      val edges: RDD[Edge[Double]] = transitionProbabilities
        .filter { case ((from, to), probability) => probability >= threshold }
        .map { case ((from, to), probability) =>
          Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
        }

      // Step 7b: Build the Graph (LPA needs Long IDs)
      val graph = Graph(activitiesRDD.mapValues(_.hashCode.toLong), edges)

      // Step 7c: Run LPA (Only using Long IDs)
      val maxIterations = 10
      val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
        (id, attr, newAttr) => math.min(attr, newAttr), // Ensure only Long values are used
        triplet => Iterator((triplet.dstId, triplet.srcAttr)), // Send messages as Long values
        (a, b) => math.min(a, b) // Merge messages as Long values
      )

      // Step 7d: Restore Activity Names After LPA
      val communityAssignments = lpaGraph.vertices
        .join(activitiesRDD)
        .map { case (id, (community, activityName)) => (community, activityName) }
        .groupByKey()
        .mapValues(_.toSet)
        .collect()

      // Step 7e: Merge clusters across thresholds
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

    // Step 8: Convert to JSON Format
    val jsonClusters = activityClusters.values.map { cluster =>
      s"""{"members": [${cluster.mkString("\"", "\", \"", "\"")}]}"""
    }.mkString("[\n", ",\n", "\n]")

    // Step 9: Write JSON to File
    val writer = new PrintWriter(new File(outputJsonPath))
    writer.write(jsonClusters)
    writer.close()

    println(s"Final merged cluster results saved to: $outputJsonPath")

    // Stop Spark Session
    spark.stop()
  }
}
