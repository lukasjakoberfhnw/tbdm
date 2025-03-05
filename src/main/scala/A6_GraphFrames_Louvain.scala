import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import java.io.PrintWriter
import java.nio.file.Paths

object A6_GraphFrames_Louvain {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("LouvainActivityClustering")
      .master("local[*]") // Run locally with all available cores
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString

    // Load CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Step 2: Create Edges based on Consecutive Rows
    import spark.implicits._

    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) })
      .values

    val edgesRDD = consecutiveRows.map { case (currentRow, nextRow) =>
      ((currentRow.getAs[String]("Activity"), nextRow.getAs[String]("Activity")), 1)
    }

    // Compute Edge Weights
    val edgeWeightsRDD = edgesRDD
      .reduceByKey(_ + _)
      .map { case ((src, dst), weight) => (src, dst, weight) }

    // Convert to DataFrame
    val edgesDF = edgeWeightsRDD.toDF("src", "dst", "weight")
    val verticesDF = df.select("Activity").distinct().withColumnRenamed("Activity", "id")

    // Create GraphFrame
    val graph = GraphFrame(verticesDF, edgesDF)

    // Run Louvain Community Detection (Label Propagation)
    val louvainResults = graph.labelPropagation.maxIter(10).run()

    // Format the Output
    val formattedOutput = louvainResults
      .groupBy("label")
      .agg(collect_list("id").as("activities"))
      .select(
        concat_ws(":", col("label"), concat_ws(",", col("activities"))).as("cluster_output")
      )
      .collect()
      .map(_.getString(0)) // Convert DataFrame to String Array

    // Print Required Output
    formattedOutput.foreach(println)

    // Optional: Save to File
    val outputPath = "A6_graphframes_louvain.txt"
    new PrintWriter(outputPath) {
      write(formattedOutput.mkString("\n"))
      close()
    }

    // Stop Spark Session
    spark.stop()
  }
}
