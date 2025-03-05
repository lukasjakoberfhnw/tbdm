import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.nio.file.Paths
import java.io.PrintWriter

// TEST: with window of 10 min + parameters for edges !! NO DFG USED!!! Time-Based Graph (Creates connections based on time proximity rather than strict sequence.)
// Loads and preprocesses IoT event log data.
// Uses a 10-minute time window to group events and track process sequences.
// Creates a time-based graph, where edge weights decay with time differences.
// Builds a GraphX model with time-weighted edges.
// Runs Label Propagation Algorithm (LPA) to detect activity clusters.
// Prints detected clusters for analysis.

object IoTGraphProcessing {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityClustering_GraphX_LabelPropagation")
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

    println("=== Initial DataFrame Schema ===")
    df.printSchema()
    df.show(5) // Display the first 5 rows

    val dfWithTimestamp = df.withColumn("timestamp", to_timestamp(col("time:timestamp"), "yyyy-MM-dd HH:mm:ssXXX"))

    // Sort by timestamp
    val sortedDF = dfWithTimestamp.orderBy("timestamp")

    // Create time windows (10-minute sliding window for grouping activities)
    val dfWithTimeWindows = sortedDF
      .withColumn("time_window", window(col("timestamp"), "10 minutes"))

    // Step 2: Create edges using a sliding window approach
    val windowSpecTime = Window.partitionBy("time_window").orderBy("timestamp")
    val dfWithTimeWindowsAndRow = dfWithTimeWindows.withColumn("row_num", row_number().over(windowSpecTime))

    val dfPairs = dfWithTimeWindowsAndRow.as("df1").join(
      dfWithTimeWindowsAndRow.as("df2"),
      col("df1.time_window") === col("df2.time_window") && col("df1.row_num") < col("df2.row_num"),
      "inner"
    ) // Frequency - directly follows a specific event -- able to build DFG in GraphX or as it is in input?
    // Outcome: list of clusters with the current event as an anchor

    val dfPairsSelected = dfPairs
      .select(
        col("df1.Activity").as("Activity1"),
        col("df2.Activity").as("Activity2"),
        col("df2.timestamp").as("timestamp2"),
        col("df1.timestamp").as("timestamp1")
      )

    val dfPairsWithTimeDiff = dfPairsSelected
      .withColumn("time_diff_seconds", unix_timestamp(col("timestamp2")) - unix_timestamp(col("timestamp1")))

    // Step 3: Create edges with time-based weights
    val edgesRDD = dfPairsWithTimeDiff.rdd.map { row =>
      val activity1 = row.getAs[String]("Activity1")
      val activity2 = row.getAs[String]("Activity2")
      val timeDiff = row.getAs[Long]("time_diff_seconds")

      // Exponential decay weighting scheme - to control strength between connections
      val decayConstant = 0.001 // The weight decreases slowly, meaning even large time differences still create significant edges
      val edgeWeight = math.exp(-decayConstant * timeDiff)

      Edge(activity1.hashCode.toLong, activity2.hashCode.toLong, edgeWeight)
    }.filter(_.attr > 0.01) // If activities occur close together in time, edgeWeight is high (close to 1).

    // Step 4: Create vertices from unique activities (without filtering by frequency)
    val verticesRDD = sortedDF.select("Activity").distinct().rdd.map(row =>
      (row.getAs[String]("Activity").hashCode.toLong, row.getAs[String]("Activity"))
    )

    // Step 5: Build the Graph
    val graph = Graph(verticesRDD, edgesRDD)

    // Print the edges
    println("=== Edges ===")
    graph.edges.collect().foreach(println)

    // Print the unique activities
    println("=== Unique Activities ===")
    graph.vertices.collect().foreach(println)

    // Step 6: Use Label Propagation for clustering
    val labelPropagation = LabelPropagation.run(graph, 5)

    // Step 7: Compute Activity Frequencies
    // This is necessary to find the most frequent activity for renaming clusters
    val activityFrequencies = df.groupBy("Activity").count().rdd
      .map(row => (row.getString(0), row.getLong(1)))
      .collectAsMap() // Convert to a lookup map

    // Step 8: Join Label Propagation results with activity names
    // Instead of using raw Cluster ID, rename each cluster based on the most frequent activity in it
    val clustersWithActivities = labelPropagation.vertices.join(verticesRDD)
      .map { case (id, (clusterId, activity)) => (clusterId, activity) }
      .groupByKey()
      .mapValues(activities => {
        val mostFrequentActivity = activities.maxBy(activity => activityFrequencies.getOrElse(activity, 0L))
        (mostFrequentActivity, activities.toSet) // Return new cluster name
      })


    // Step 9: Print and Save Renamed Clusters to a File
    val outputFilePath = "A4_iot_graph_processing.txt" // Change this path if needed
    val writer = new PrintWriter(outputFilePath)

    println("=== Renamed Clusters (Based on Most Frequent Activity) ===")

    clustersWithActivities.collect().foreach { case (clusterId, (mostFrequentActivity, activities)) =>
      val activityList = activities.mkString(",") // Convert activities to comma-separated values
      val clusterLine = s"$clusterId:$activityList" // Format as required

      println(clusterLine)  // Print to console
      writer.println(clusterLine) // Write to file
    }

    writer.close() // Close the file writer
    println(s"Clusters saved to: $outputFilePath")


    // Stop Spark session
    spark.stop()
  }
}
