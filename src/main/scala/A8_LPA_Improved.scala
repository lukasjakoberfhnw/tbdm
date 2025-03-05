import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}
import scala.collection.mutable
import java.nio.file.Paths
import java.io.PrintWriter

object A8_LPA_Improved {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityGraphFinalClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val outputJsonPath = "A8_lpa_improved.json"
    val outputTxtPath = "A8_lpa_improved.txt"

    // Step 1: Load CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Step 2: Extract Distinct Activities (Vertices)
    val activitiesRDD: RDD[(VertexId, String)] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, activity) // Store ID as Long
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

    // Step 6: Dictionary to Store Co-Occurrences
    val coOccurrenceMap = mutable.Map[(String, String), Int]()

    // Step 7: Loop Over Thresholds
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
        .mapValues(_.toList)
        .collect()

      communityAssignments.foreach { case (_, activities) =>
        for (a <- activities; b <- activities if a != b) {
          val key = if (a < b) (a, b) else (b, a)
          coOccurrenceMap(key) = coOccurrenceMap.getOrElse(key, 0) + 1
        }
      }
    }

    // Step 8: Convert Co-Occurrence Map to Final Clusters
    val finalClusters = mutable.Map[String, mutable.Set[String]]()

    coOccurrenceMap.foreach { case ((activity1, activity2), count) =>
      if (count > (probabilityThresholds.size / 2)) {
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

    // Step 9: Convert to JSON Format
    val jsonClusters = finalClusters.values.map { cluster =>
      s"""{"members": [${cluster.mkString("\"", "\", \"", "\"")}]}"""
    }.mkString("[\n", ",\n", "\n]")

    // Step 10: Write JSON to File
    val writer = new PrintWriter(new File(outputJsonPath))
    writer.write(jsonClusters)
    writer.close()

    println(s"Final cluster results saved to: $outputJsonPath")

    // Step 11: Write TXT file with required format
    val txtWriter = new PrintWriter(new File(outputTxtPath))
    finalClusters.zipWithIndex.foreach { case ((clusterId, activities), index) =>
      txtWriter.println(s"$index:${activities.mkString(",")}")
    }
    txtWriter.close()

    println(s"Final cluster results saved to: $outputTxtPath")

    // Stop Spark Session
    spark.stop()
  }
}