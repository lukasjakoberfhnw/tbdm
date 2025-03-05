import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.io.PrintWriter
import scala.collection.mutable
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.DefaultFormats

object A1_ActivityClustering {
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

    // Step 4: Build clusters
    val clusters = mutable.Map[String, Set[String]]()
    relationshipsCollected.flatMap { case (relationship, _) =>
      val Array(sourceActivity, targetActivity) = relationship.split("/")
      Seq(sourceActivity, targetActivity)
    }.distinct.foreach(activity => clusters(activity) = Set(activity))

    // Create a priority queue for relationships (sorted by count in descending order)
    val relationshipQueue = mutable.PriorityQueue[(String, String, Int)]()(
      Ordering.by(_._3) // Sort by the count (highest first)
    )

    // Populate the queue
    relationshipQueue ++= relationshipsCollected.map { case (relationship, count) =>
      val Array(sourceActivity, targetActivity) = relationship.split("/")
      (sourceActivity, targetActivity, count)
    }

    // Iteratively merge clusters
    breakable {
      while (clusters.size > 15 && relationshipQueue.nonEmpty) {
        val (source, target, count) = relationshipQueue.dequeue()

        // Stop if the count is less than 2
        if (count < 2) { // 5 was a good number
          println(s"Stopping clustering as relationship count dropped below threshold: $count")
          break
        }

        // Find clusters containing source and target
        val sourceCluster = clusters.find { case (_, activities) => activities.contains(source) }
        val targetCluster = clusters.find { case (_, activities) => activities.contains(target) }

        // Merge clusters if source and target are in different clusters
        if (sourceCluster.isDefined && targetCluster.isDefined && sourceCluster.get != targetCluster.get) {
          val (sourceKey, sourceActivities) = sourceCluster.get
          val (targetKey, targetActivities) = targetCluster.get

          // Check cluster size constraint
          if (sourceActivities.size + targetActivities.size > 4) {
            println(s"Skipping merge: Combined cluster size exceeds 4 (Source: ${sourceActivities.size}, Target: ${targetActivities.size})")
            // Skip this merge
          } else {
            // Merge the clusters
            val mergedCluster = sourceActivities ++ targetActivities
            clusters(sourceKey) = mergedCluster
            clusters.remove(targetKey)

            println(s"Merged clusters: $sourceKey (${sourceActivities.mkString(", ")}) " +
              s"and $targetKey (${targetActivities.mkString(", ")}) -> " +
              s"New Cluster: ${mergedCluster.mkString(", ")}")
          }
        }
      }
    }

        // Convert to required string format
    val clustersString = clusters.zipWithIndex.map { case ((_, activities), index) =>
      s"$index:${activities.mkString("," )}"
    }.mkString("\n")
    
    // Print to console
    println(clustersString)
        
    // Save to file
    new PrintWriter("clusters.txt") { write(clustersString); close() }
    println("Final clusters saved to clusters.txt")



    // // Step 5: Print final clusters
    // println("=== Final Clusters (15 Remaining) ===")
    // clusters.zipWithIndex.foreach { case ((_, activities), index) =>
    //   println(s"Cluster $index:")
    //   activities.foreach(activity => println(s"  - $activity"))
    // }

    // // Step 6: Store clusters as .txt file

    // println("=== Store Clusters in txt file ===")
    // val outputText = new StringBuilder // Use StringBuilder instead of String

    // clusters.zipWithIndex.foreach { case ((_, activities), index) =>
    //   outputText.append(s"Cluster $index: ") // Append instead of using +=
    //   outputText.append(activities.mkString(", ")) 
    //   outputText.append("\n")
    // }

    // new PrintWriter("out.txt") { write(outputText.toString()); close }

    // temporary: store to csv

    /* 
    // Step 6: Save clusters to a CSV file
    val clusterRows = clusters.zipWithIndex.flatMap { case ((_, activities), clusterId) =>
      activities.map(activity => Row(clusterId.toString, activity))
    }.toSeq

    // Define schema for the DataFrame
    val schema = StructType(Seq(
      StructField("ClusterID", StringType, nullable = false),
      StructField("Activity", StringType, nullable = false)
    ))

    // Create DataFrame from rows
    val clusterDF = spark.createDataFrame(spark.sparkContext.parallelize(clusterRows), schema)

    // Save DataFrame to a CSV file
    val outputCsvPath = "/tmp/final_clusters.csv"
    clusterDF.write
      .option("header", "true") // Include header in CSV
      .csv(outputCsvPath)

    println(s"Final clusters saved to: $outputCsvPath")
    */

    // Stop Spark Session
    spark.stop()
  }
}
