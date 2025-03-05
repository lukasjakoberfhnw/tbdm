import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.util.Random
import java.nio.file.Paths

object GraphX_ImprovedInfomap {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("GraphX_Infomap")
      .master("local[*]") // Run locally
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext

    val csvPath = Paths.get("data", "sorted_logfile.csv").toString

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

    // Step 3: Compute Edge Weights (Normalize by Total Outgoing Edges)
    val rawEdges: RDD[(VertexId, VertexId)] = consecutiveRows.map { case (currentRow, nextRow) =>
      (currentRow.getAs[String]("Activity").hashCode.toLong, 
       nextRow.getAs[String]("Activity").hashCode.toLong)
    }

    val edgeCounts = rawEdges.map(e => (e._1, 1)).reduceByKey(_ + _)
    val edges: RDD[Edge[Double]] = rawEdges
      .join(edgeCounts)
      .map { case (src, (dst, count)) =>
        Edge(src, dst, 1.0 / count) // Normalize probability by outgoing edges
      }

    // Step 4: Define Vertices
    val vertices: RDD[(VertexId, (String, Double))] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, (activity, 1.0)) // Initial probability = 1.0
    }).distinct()

    // Step 5: Create Graph
    var graph = Graph(vertices, edges)

    // Step 6: Run Infomap Clustering with Normalization and Decay
    val numIterations = 15
    val decayFactor = 0.85 // Keeps clusters stable and prevents fragmentation

    for (_ <- 1 to numIterations) {
      // Step 6a: Propagate Information Flow with Weighted Edges
      val newRanks = graph.aggregateMessages[Double](
        triplet => {
          triplet.sendToDst(triplet.srcAttr._2 * triplet.attr) // Weight-based flow
        },
        _ + _
      )

      // Step 6b: Normalize Probabilities & Apply Decay
      graph = graph.outerJoinVertices(newRanks) {
        case (_, (activity, oldProb), Some(newProb)) => (activity, decayFactor * newProb + (1 - decayFactor) * oldProb)
        case (_, (activity, oldProb), None) => (activity, oldProb) // No change if no messages received
      }
    }

    // Step 7: Assign Clusters Based on Final Probabilities
    val clusters = graph.vertices.map {
      case (id, (activity, prob)) => (math.round(prob * 1000).toInt, activity) // Scale for clustering
    }.groupByKey()
      .map { case (probGroup, activities) =>
        (probGroup, activities.toList)
      }

    // Print results
    println("=== Improved Infomap Clusters ===")
    clusters.collect().foreach { case (clusterScore, activities) =>
      println(s"Cluster Score: $clusterScore")
      activities.foreach(activity => println(s"  - $activity"))
    }

    // Step 8: Save Results to JSON
    val jsonResults = clusters.map { case (score, activities) =>
      s"""{"score": $score, "activities": ["${activities.mkString("\", \"")}"]}"""
    }.collect().mkString("[", ",", "]")

    val outputPath = "graphx_infomap_improved.json"
    new PrintWriter(outputPath) {
      write(jsonResults)
      close()
    }

    println(s"Results saved to $outputPath")

    // Stop Spark Session
    spark.stop()
  }
}
