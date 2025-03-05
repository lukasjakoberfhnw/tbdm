import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random

object Lukas_Clustering3 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityGraphLPA")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = "/home/lukas/temp/sorted_logfile.csv"

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
      .map { case ((from, to), count) => (from, (to, count)) } // Change key to match totalOutgoingCounts
      .join(totalOutgoingCounts) // Join by "from" activity
      .map { case (from, ((to, count), totalCount)) =>
        ((from, to), count.toDouble / totalCount.toDouble) // Compute probability correctly
      }

    // Step 6: Create GraphX Edges (Filter out 0-probability edges)
    val edges: RDD[Edge[Double]] = transitionProbabilities
      .filter { case ((from, to), probability) => probability > 0 } // Exclude zero probability
      .map { case ((from, to), probability) =>
        Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
      }

    // Step 7: Build the Graph with numerical community labels
    val graph = Graph(activitiesRDD.mapValues(_._2), edges)

    // Step 8: Run Label Propagation Algorithm (LPA)
    val maxIterations = 10
    val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
      (id, attr, newAttr) => math.min(attr, newAttr), // Ensure only Long values are used
      triplet => Iterator((triplet.dstId, triplet.srcAttr)), // Send messages as Long values
      (a, b) => math.min(a, b) // Merge messages as Long values
    )

    // Step 9: Restore Activity Names
    val communityAssignments = lpaGraph.vertices
      .join(activitiesRDD) // Join back to original activity names
      .map { case (id, (community, (activityName, _))) => (community, activityName) } // Use community as key
      .groupByKey()
      .mapValues(_.toList)

    // Step 10: Print Detected Communities
    println("=== Detected Communities ===")
    communityAssignments.collect().foreach { case (community, activities) =>
      println(s"Community $community: ${activities.mkString(", ")}")
    }

    // Stop Spark Session
    spark.stop()
  }
}
