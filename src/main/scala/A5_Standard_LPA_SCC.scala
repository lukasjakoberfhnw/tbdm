import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.LabelPropagation
import java.io.PrintWriter
import java.nio.file.Paths

// APPROACH 1 - results - all single clusters
// Loads and processes the event log dataset.
// Builds a Directly-Follows Graph (DFG) where activities are linked in sequence.
// Converts the data into GraphX format (vertices and edges).
// Runs two GraphX clustering algorithms:
// - Label Propagation Algorithm (LPA)
// - Strongly Connected Components (SCC)
// Prints the detected clusters of related activities.
// Saves the final clusters to a file.

object A5_Standard_LPA_SCC {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("DFG_GraphX_Clustering")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val csvPath = Paths.get("data", "sorted_logfile.csv").toString

    // Step 1: Load CSV and adjust timestamps
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    val dfWithTimestamp = df.withColumn("timestamp", to_timestamp(col("time:timestamp"), "yyyy-MM-dd HH:mm:ssXXX"))

    // Step 2: Construct the Directly-Follows Graph (DFG)
    val windowSpecDFG = Window.partitionBy("concept:name").orderBy("timestamp")

    val dfWithNextActivity = dfWithTimestamp
      .withColumn("next_activity", lead(col("Activity"), 1).over(windowSpecDFG))
      .filter(col("next_activity").isNotNull)

    val dfgEdgesDF = dfWithNextActivity
      .groupBy("Activity", "next_activity")
      .agg(count("*").alias("dfg_weight"))

    // Step 3: Convert to GraphX RDDs
    val verticesRDD: RDD[(VertexId, String)] = dfgEdgesDF
      .select("Activity").distinct().rdd
      .map(row => (row.getString(0).hashCode.toLong, row.getString(0)))

    val edgesRDD: RDD[Edge[Double]] = dfgEdgesDF
      .rdd.map(row =>
        Edge(
          row.getString(0).hashCode.toLong,  // Source (Activity)
          row.getString(1).hashCode.toLong,  // Destination (Next Activity)
          row.getLong(2).toDouble            // Edge weight (DFG frequency)
        )
      )

    // Step 4: Build the Graph
    val graph = Graph(verticesRDD, edgesRDD)

    // Step 5: Compute Activity Frequencies (For Naming Clusters)
    val activityFrequencies = df.groupBy("Activity").count().rdd
      .map(row => (row.getString(0), row.getLong(1)))
      .collectAsMap() // Convert to lookup map

    // Step 6: Apply Different GraphX Algorithms
    def printAndSaveClusters(title: String, clusterResults: RDD[(VertexId, VertexId)], outputFilePath: String): Unit = {
      val writer = new PrintWriter(outputFilePath)

      val clustersWithNames = clusterResults.join(verticesRDD)
        .map { case (id, (clusterId, activity)) => (clusterId, activity) }
        .groupByKey()
        .mapValues(activities => {
          val mostFrequentActivity = activities.maxBy(activity => activityFrequencies.getOrElse(activity, 0L))
          (mostFrequentActivity, activities)
        })

      clustersWithNames.collect().foreach { case (clusterId, (_, activities)) =>
        val activityList = activities.mkString(",")  // Format activities as comma-separated values
        val clusterLine = s"$clusterId:$activityList"  // Format as required
        
        println(clusterLine)  // Print to console
        writer.println(clusterLine)  // Write to file
      }

      writer.close()
      println(s"Clusters saved to: $outputFilePath")
    }

    // Step 7: Run Clustering Algorithms and Save Results
    val labelPropagation = LabelPropagation.run(graph, 5)
    printAndSaveClusters("Label Propagation", labelPropagation.vertices, "A5_lpa_clusters.txt")

    val stronglyConnected = graph.stronglyConnectedComponents(5)
    printAndSaveClusters("Strongly Connected Components", stronglyConnected.vertices, "A5_scc_clusters.txt")

    spark.stop()
  }
}
