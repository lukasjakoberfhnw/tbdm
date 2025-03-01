/*
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.util.control.Breaks._

object ActivityNeighbourhood {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("ActivityClustering")
      .master("local[*]") // Run locally with all available cores
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val csvPath = "/tmp/data/sorted_logfile.csv"

    // Step 1: Load CSV and inspect schema
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    println("=== Initial DataFrame Schema ===")
    df.printSchema()
    df.show(5) // Display the first 5 rows

    // Step 2: Prepare data with expanded neighborhood
    val activities = df.select("Activity").rdd.map(_.getString(0)).collect()
    val windowSize = 3 // Define neighborhood size

    val neighborhoods = activities.indices.map { idx =>
      val activity = activities(idx)
      val prevActivities = activities.slice((idx - windowSize).max(0), idx) // 3 previous
      val nextActivities = activities.slice(idx + 1, (idx + 1 + windowSize).min(activities.length)) // 3 next
      (activity, prevActivities ++ nextActivities) // Combine previous and next activities
    }

    // Step 3: Aggregate neighborhoods into relationships
    val neighborhoodRelationships = neighborhoods.flatMap { case (activity, neighbors) =>
      neighbors.map(neighbor => (activity, neighbor)) // Create pair for each neighbor
    }.groupBy(identity) // Group by pair
      .map { case ((activity, neighbor), occurrences) => (activity, neighbor, occurrences.size) } // Count occurrences

    println("=== Neighborhood Relationships ===")
    neighborhoodRelationships.foreach { case (activity, neighbor, count) =>
      println(s"Activity: $activity -> Neighbor: $neighbor, Count: $count")
    }

    // Step 4: Build clusters
    val clusters = mutable.Map[String, Set[String]]()
    neighborhoodRelationships.flatMap { case (activity, neighbor, _) =>
      Seq(activity, neighbor)
    }.distinct.foreach(activity => clusters(activity) = Set(activity))

    // Create a priority queue for relationships (sorted by count in descending order)
    val relationshipQueue = mutable.PriorityQueue[(String, String, Int)]()(
      Ordering.by(_._3) // Sort by count (highest first)
    )
    relationshipQueue ++= neighborhoodRelationships

    // Iteratively merge clusters
    breakable {
      while (clusters.size > 15 && relationshipQueue.nonEmpty) {
        val (source, target, count) = relationshipQueue.dequeue()

        // Stop if the count is less than 2
        if (count < 2) {
          println(s"Stopping clustering as relationship count dropped below threshold: $count")
          break
        }

        // Find clusters containing source and target
        val sourceCluster = clusters.find { case (_, activities) => activities.contains(source) }
        val targetCluster = clusters.find { case (_, activities) => activities.contains(target) }

        // Merge clusters if source and target are in different clusters and size constraint is met
        if (sourceCluster.isDefined && targetCluster.isDefined && sourceCluster.get != targetCluster.get) {
          val (sourceKey, sourceActivities) = sourceCluster.get
          val (targetKey, targetActivities) = targetCluster.get

          // Check cluster size constraint
          if (sourceActivities.size + targetActivities.size > 4) {
            println(s"Skipping merge: Combined cluster size exceeds 4 (Source: ${sourceActivities.size}, Target: ${targetActivities.size})")
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

    // Step 5: Print final clusters
    println("=== Final Clusters (15 Remaining) ===")
    clusters.zipWithIndex.foreach { case ((_, activities), index) =>
      println(s"Cluster $index:")
      activities.foreach(activity => println(s"  - $activity"))
    }

    // Stop Spark Session
    spark.stop()
  }
}
*/