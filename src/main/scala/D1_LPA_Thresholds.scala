import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import java.nio.file.Paths
import java.io.PrintWriter

object D1_LPA_Thresholds {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityGraphLPA")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val outputJsonPath = "D1_lpa_tresholds.json"

    // Step 1: Load CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Step 2: Extract Distinct Activities (Vertices)
    val activitiesRDD: RDD[(VertexId, (String, Long))] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, (activity, activity.hashCode.toLong)) // Store ID as Long
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

    // Step 6: Loop over probability thresholds and store results
    val results = scala.collection.mutable.ListBuffer[String]()
    val probabilityThresholds = 0.05 to 1.0 by 0.05

    for (threshold <- probabilityThresholds) {
      // Step 6a: Filter edges based on threshold
      val edges: RDD[Edge[Double]] = transitionProbabilities
        .filter { case ((from, to), probability) => probability >= threshold }
        .map { case ((from, to), probability) =>
          Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
        }

      // Step 6b: Build the Graph
      val graph = Graph(activitiesRDD.mapValues(_._2), edges)

      // Step 6c: Run LPA
      val maxIterations = 10
      val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
        (id, attr, newAttr) => math.min(attr, newAttr),
        triplet => Iterator((triplet.dstId, triplet.srcAttr)),
        (a, b) => math.min(a, b)
      )

      // Step 6d: Restore Activity Names
      val communityAssignments = lpaGraph.vertices
        .join(activitiesRDD)
        .map { case (id, (community, (activityName, _))) => (community, activityName) }
        .groupByKey()
        .mapValues(_.toList)

      // Step 6e: Convert to JSON Format
      val jsonClusters = communityAssignments.collect().map { case (community, activities) =>
        s"""{"community": "$community", "members": [${activities.mkString("\"", "\", \"", "\"")}]}"""
      }.mkString("[\n", ",\n", "\n]")

      results.append(s"""{"threshold": $threshold, "clusters": $jsonClusters}""")
    }

    // Step 7: Write JSON to File
    val writer = new PrintWriter(new File(outputJsonPath))
    writer.write("[\n" + results.mkString(",\n") + "\n]")
    writer.close()

    println(s"Cluster results saved to: $outputJsonPath")

    // Stop Spark Session
    spark.stop()
  }
}
