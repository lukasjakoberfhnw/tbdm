import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.nio.file.Paths

object A3_GraphX_ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ActivityClustering")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString
    val outputPath = "A3_graphx_connectedcomponents.txt" 

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

    // Step 1: Count occurrences of each activity transition
    val threshold = 9  // Minimum number of occurrences required to form an edge
    val filteredEdges = rawEdges
      .reduceByKey(_ + _)
      .filter { case (_, count) => count >= threshold }  // Keep only frequent transitions

    // Step 2: Convert to EdgeRDD for GraphX
    val edges: RDD[Edge[Int]] = filteredEdges.map { case ((src, dst), count) =>
      Edge(src.hashCode.toLong, dst.hashCode.toLong, count)
    }

    // Step 3: Create vertices
    val vertices: RDD[(VertexId, String)] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, activity)
    }).distinct()

    // Step 4: Create Graph and Compute Connected Components
    val graph = Graph(vertices, edges)
    val connectedComponents = graph.connectedComponents().vertices

    // Step 5: Join components with activity names
    val clusters = connectedComponents.join(vertices).map {
      case (_, (clusterId, activity)) => (clusterId, activity)
    }.groupByKey()
      .map { case (clusterId, activities) =>
        s"$clusterId:${activities.mkString(", ")}"
      }.collect()

    // Save clusters to a text file
    val writer = new PrintWriter(outputPath)
    try {
      clusters.foreach(writer.println)
    } finally {
      writer.close()
    }

    println(s"Clusters saved to $outputPath")

    spark.stop()
  }
}
