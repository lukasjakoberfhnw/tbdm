import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphX Example").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val csvPath = "/tmp/data/sorted_logfile.csv"
    val csvRDD = sc.textFile(csvPath)

    val header = csvRDD.first()
    val dataRDD = csvRDD.filter(row => row != header)
    dataRDD.collect().foreach(row => println(row.mkString(", ")))
  
    // Define vertices
    val vertices: RDD[(VertexId, String)] = sc.parallelize(Array(
      (1L, "Alice"),
      (2L, "Bob"),
      (3L, "Charlie"),
      (4L, "Lukas"),
      (5L, "Arbnor"),
      (6L, "Samuel")
    ))

    // Define edges
    val edges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(1L, 2L, "Friend"),
      Edge(2L, 3L, "Colleague"),
      Edge(3L, 4L, "Friend"),
      Edge(4L, 5L, "Friend"),
      Edge(5L, 6L, "Friend")
    ))

    // Create a graph
    val graph = Graph(vertices, edges)

    // Print the graph
    println("Vertices:")
    graph.vertices.collect.foreach(println)
    println("Edges:")
    graph.edges.collect.foreach(println)

    sc.stop()
  }
}
