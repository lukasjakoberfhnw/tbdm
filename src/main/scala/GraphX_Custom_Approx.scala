import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.{File, PrintWriter}

object GraphX_Custom_Approx {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("GraphX_Infomap")
      .master("local[*]") // Run locally
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = "/home/lukas/temp/sorted_logfile.csv"
    val outputJsonPath = "communities.json"

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

    // Step 3: Define Vertices (Convert String to Long)
    val vertices: RDD[(VertexId, Long)] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, activity.hashCode.toLong) // Store ID as Long
    }).distinct()

    // Step 4: Create Graph
    var graph = Graph(vertices, edges)

    // Step 5: Community Detection using Label Propagation (Approximating Infomap)
    val maxIterations = 10
    val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
      (id, attr, newAttr) => math.min(attr, newAttr), // Ensure attributes are Long
      triplet => Iterator((triplet.dstId, triplet.srcAttr)), // Send message
      (a, b) => math.min(a, b) // Merge messages
    )

    // Step 6: Extract Communities
    val communities = lpaGraph.vertices.map { case (id, communityLabel) =>
      (communityLabel, id)
    }.groupByKey().mapValues(_.toList)

    // Step 7: Convert to JSON Format
    val communityList = communities.collect().map { case (community, members) =>
      s"""{"community": "$community", "members": [${members.mkString("\"", "\", \"", "\"")}]}"""
    }.mkString("[\n", ",\n", "\n]")

    // Step 8: Write JSON to File
    val writer = new PrintWriter(new File(outputJsonPath))
    writer.write(communityList)
    writer.close()

    println(s"Communities saved as JSON: $outputJsonPath")

    // Stop Spark Session
    spark.stop()
  }
}
