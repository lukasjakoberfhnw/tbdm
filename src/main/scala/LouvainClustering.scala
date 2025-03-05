import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.collection.mutable

object GraphX_Louvain {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("LouvainActivityClustering")
      .master("local[*]") // Run locally with all available cores
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val csvPath = "/home/lukas/temp/sorted_logfile.csv"

    // Step 1: Load CSV and inspect schema
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    println("=== Initial DataFrame Schema ===")
    df.printSchema()
    df.show(5) // Display the first 5 rows

    // Step 2: Create Edges based on Consecutive Rows
    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) }) // Offset by -1
      .values

    val edgesRDD = consecutiveRows.map { case (currentRow, nextRow) =>
      ((currentRow.getAs[String]("Activity"), nextRow.getAs[String]("Activity")), 1)
    }

    // Step 3: Compute Edge Weights (How Often Each Transition Appears)
    val edgeWeightsRDD = edgesRDD
      .reduceByKey(_ + _)
      .map { case ((src, dst), weight) => (src, dst, weight) }

    // Convert RDD to DataFrame
    import spark.implicits._
    val edgesDF = edgeWeightsRDD.toDF("src", "dst", "weight")

    // Step 4: Create Vertices (Unique Activities)
    val verticesDF = df.select("Activity").distinct()
      .withColumnRenamed("Activity", "id")

    println("=== Vertices (Unique Activities) ===")
    verticesDF.show(10, false)

    println("=== Edges with Weights ===")
    edgesDF.show(10, false)

    // Step 5: Create GraphFrame
    val graph = GraphFrame(verticesDF, edgesDF)

    // Step 6: Run Louvain Community Detection (Label Propagation)
    val louvainResults = graph.labelPropagation.maxIter(10).run()

    println("=== Louvain Communities ===")
    louvainResults.select("id", "label").show(50, false)

   val jsonResults = louvainResults.toJSON.collect().mkString("[", ",", "]")  // Convert entire DataFrame to single JSON string

    val outputPath = "louvain_clusters.json"
    new PrintWriter(outputPath) {
      write(jsonResults)
      close()
    }
    // Stop Spark Session
    spark.stop()
  }
}
