import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.util.Random

object Lukas_Custom {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityClustering")
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

    // Step 2: Create edges based on consecutive rows
    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) }) // Offset by -1
      .values

    val edges = consecutiveRows.map { case (currentRow, nextRow) =>
      Edge(
        currentRow.getAs[String]("Activity").hashCode.toLong, // Source vertex ID
        nextRow.getAs[String]("Activity").hashCode.toLong,   // Destination vertex ID
        currentRow.getAs[String]("Activity") + "/" + nextRow.getAs[String]("Activity") // Edge attribute
      )
    }

    // Step 3: Compute relationship counts
    val edgeRelationships = edges.map(edge => edge.attr)
    val relationshipCounts = edgeRelationships.map(relationship => (relationship, 1))
      .reduceByKey(_ + _)

    println("=== Relationship Counts ===")
    val relationshipsCollected = relationshipCounts.collect()
    relationshipsCollected.foreach { case (relationship, count) =>
      println(s"Relationship: $relationship, Count: $count")
    }

    // Stop Spark Session
    spark.stop()
  }
}
