import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.LabelPropagation
import java.io.PrintWriter

// APPROACH 2 - results 10 Clusters - DFG used
// Loads and preprocesses event log data.
// Builds a Directly-Follows Graph (DFG) from activity sequences.
// Converts the DFG into a GraphX model (vertices & edges).
// Runs Label Propagation Algorithm (LPA) for clustering.
// Refines clusters by splitting large ones into smaller groups.
// Prints the final set of activity clusters and saves them to a file.

object Sol4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Optimized Process Mining (GraphX Only)")
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Load and preprocess data
    val df = spark.read
      .option("header", "true")
      .csv("/home/lukas/temp/sorted_logfile.csv") // Replace with your file path
      .withColumn("timestamp", to_timestamp(col("time:timestamp")))
      .withColumn("synthetic_case_id",
        date_format(col("timestamp"), "yyyyMMdd")
      )

    // 2. Create Directly-Follows Graph edges
    val windowSpec = Window
      .partitionBy("synthetic_case_id")
      .orderBy("timestamp")

    val edgesDF = df
      .withColumn("next_activity", lead(col("Activity"), 1).over(windowSpec))
      .filter(col("next_activity").isNotNull)
      .filter(col("Activity") =!= col("next_activity"))
      .groupBy("Activity", "next_activity")
      .agg(count("*").alias("weight"))
      .filter(col("weight") > 1)
      .cache()

    // 3. Build GraphX structure
    val vertices: RDD[(VertexId, String)] = edgesDF
      .select("Activity")
      .union(edgesDF.select("next_activity"))
      .distinct()
      .rdd
      .map(row => (row.getString(0).hashCode.toLong, row.getString(0)))
      .cache()

    val edges: RDD[Edge[Double]] = edgesDF.rdd.map(row =>
      Edge(
        row.getString(0).hashCode.toLong,
        row.getString(1).hashCode.toLong,
        math.log(row.getLong(2) + 1)
      )
    ).cache()

    val directedGraph = Graph(vertices, edges)

    // 4. Run Label Propagation
    val lpaGraph = LabelPropagation.run(directedGraph, 5)
    val clusters = lpaGraph.vertices
      .join(vertices)
      .map { case (_, (cid, activity)) => (cid, activity) }
      .groupByKey()
      .map { case (cid, acts) => (cid.abs, acts.toSet) }
      .filter { case (_, activities) => activities.size >= 2 }

    // 5. Refine clusters
    val refinedClusters = refineClusters(clusters)

    // 6. Compute Activity Frequencies (For Naming Clusters)
    val activityFrequencies = df.groupBy("Activity").count().rdd
      .map(row => (row.getString(0), row.getLong(1)))
      .collectAsMap() // Convert to lookup map

    // 7. Rename Clusters Based on Most Frequent Activity
    val renamedClusters = refinedClusters.map { case (clusterId, activities) =>
      val mostFrequentActivity = activities.maxBy(activity => activityFrequencies.getOrElse(activity, 0L))
      (mostFrequentActivity, activities)
    }

    // 8. Print and Save Renamed Clusters to a File
    val outputFilePath = "out_sol4_clusters.txt" // Change this path if needed
    val writer = new PrintWriter(outputFilePath)

    println("=== Renamed Clusters (Based on Most Frequent Activity) ===")
    writer.println("=== Renamed Clusters (Based on Most Frequent Activity) ===")

    renamedClusters.collect().foreach { case (mostFrequentActivity, activities) =>
      println(s"""Cluster "$mostFrequentActivity" """)
      writer.println(s"""Cluster "$mostFrequentActivity" """)

      activities.foreach { activity =>
        println(s"  - $activity")
        writer.println(s"  - $activity")
      }
    }

    writer.close() // Close the file writer
    println(s"Clusters saved to: $outputFilePath")

    spark.stop()
  }

  def refineClusters(clusters: RDD[(VertexId, Set[String])]): RDD[(VertexId, Set[String])] = {
    clusters.flatMap { case (cid, activities) =>
      if (activities.size > 10) {
        activities.grouped(5).zipWithIndex.map { case (group, idx) =>
          (cid + idx, group.toSet)
        }
      } else {
        Seq((cid, activities))
      }
    }
  }
}