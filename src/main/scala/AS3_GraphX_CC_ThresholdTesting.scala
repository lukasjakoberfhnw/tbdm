import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.io.PrintWriter
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.DefaultFormats
import java.nio.file.Paths

object AS3_GraphX_CC_ThresholdTesting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ActivityClusteringThresholdTest")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) }) // Offset by -1
      .values

    val rawEdges = consecutiveRows.map { case (currentRow, nextRow) =>
      ((currentRow.getAs[String]("Activity"), nextRow.getAs[String]("Activity")), 1)
    }

    // Count occurrences of each activity transition
    val edgeCounts = rawEdges.reduceByKey(_ + _)

    // Store results for each threshold
    var results = scala.collection.mutable.Map[Int, Int]()

    for (threshold <- 0 to 100) {
      val filteredEdges = edgeCounts.filter { case (_, count) => count >= threshold }

      val edges: RDD[Edge[Int]] = filteredEdges.map { case ((src, dst), count) =>
        Edge(src.hashCode.toLong, dst.hashCode.toLong, count)
      }

      val vertices: RDD[(VertexId, String)] = df.rdd.map(row => {
        val activity = row.getAs[String]("Activity")
        (activity.hashCode.toLong, activity)
      }).distinct()

      val graph = Graph(vertices, edges)
      val connectedComponents = graph.connectedComponents().vertices

      val numClusters = connectedComponents.map(_._2).distinct().count().toInt
      results(threshold) = numClusters

      println(s"Threshold: $threshold, Number of Clusters: $numClusters")
    }

    // Write results to JSON file
    implicit val formats: DefaultFormats.type = DefaultFormats
    val jsonString = write(results)

    val writer = new PrintWriter("threshold_results.json")
    writer.write(jsonString)
    writer.close()

    println("Results saved to clusters_result.json")

    spark.stop()
  }
}
