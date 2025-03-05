import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.util.Random

object GraphX_Infomap {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("GraphX_Infomap")
      .master("local[*]") // Run locally
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = "/home/lukas/temp/sorted_logfile.csv"

    // Step 1: Load CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Step 2: Create Edges
    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) }) // Offset by -1
      .values

    val edges: RDD[Edge[Double]] = consecutiveRows.map { case (currentRow, nextRow) =>
      Edge(
        currentRow.getAs[String]("Activity").hashCode.toLong, // Source vertex ID
        nextRow.getAs[String]("Activity").hashCode.toLong,   // Destination vertex ID
        Random.nextDouble() // Initial random probability (simulating information flow)
      )
    }

    // Step 3: Define Vertices
    val vertices: RDD[(VertexId, (String, Double))] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, (activity, 1.0)) // Initial probability = 1.0
    }).distinct()

    // Step 4: Create Graph
    var graph = Graph(vertices, edges)


    // Stop Spark Session
    spark.stop()
  }
}
