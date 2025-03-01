import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.util.control.Breaks._

object ActivityOverlapping {
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

    // Step 2: Prepare data with expanded neighborhood (3 previous and 3 following)
    val indexedRDD = df.rdd.zipWithIndex()
    val expandedEdges = indexedRDD.flatMap { case (row, index) =>
      val activity = row.getAs[String]("Activity")
      val neighbors = (-1 to 3).filter(_ != 0).map(offset => (index + offset, activity)) // Expanded neighborhood
      neighbors
    }.join(indexedRDD.map(_.swap))
      .map { case (_, (activity, neighborRow)) =>
        val neighborActivity = neighborRow.getAs[String]("Activity")
        (activity, neighborActivity)
      }

    // Step 3: Compute relationship counts
    val relationshipCounts = expandedEdges.map { case (source, target) =>
      (s"$source/$target", 1)
    }.reduceByKey(_ + _)

    println("=== Relationship Counts ===")
    val relationshipsCollected = relationshipCounts.collect()
    relationshipsCollected.foreach { case (relationship, count) =>
      println(s"Relationship: $relationship, Count: $count")
    }

    // Step 4: Build clusters
    val clusters = mutable.Map[String, mutable.Set[String]]() // Activity -> Set of clusters
    val clusterIdToActivities = mutable.Map[String, mutable.Set[String]]() // Cluster ID -> Activities
    var nextClusterId = 0

    // Create a priority queue for relationships (sorted by count in descending order)
    val relationshipQueue = mutable.PriorityQueue[(String, String, Int)]()(
      Ordering.by(_._3) // Sort by count (highest first)
    )
    relationshipQueue ++= relationshipsCollected.map { case (relationship, count) =>
      val Array(sourceActivity, targetActivity) = relationship.split("/")
      (sourceActivity, targetActivity, count)
    }

    // Iteratively merge clusters with size and count constraints
    val significanceThreshold = 10 // Define the threshold for significant relationships
    breakable {
      while (relationshipQueue.nonEmpty) {
        val (source, target, count) = relationshipQueue.dequeue()

        // Stop if the count is less than 2
        if (count < 2) {
          println(s"Stopping clustering as relationship count dropped below threshold: $count")
          break
        }

        // Determine if this is a significant relationship
        val isSignificant = count >= significanceThreshold

        // Generate unique cluster IDs as strings
        val sourceClusterId = s"Cluster_$nextClusterId"
        val targetClusterId = s"Cluster_$nextClusterId"

        // Find or create clusters for source and target
        val sourceClusters = clusters.getOrElseUpdate(source, mutable.Set(sourceClusterId))
        if (sourceClusters.isEmpty) {
          clusterIdToActivities(sourceClusterId) = mutable.Set(source)
          nextClusterId += 1
        }
        val targetClusters = clusters.getOrElseUpdate(target, mutable.Set(targetClusterId))
        if (targetClusters.isEmpty) {
          clusterIdToActivities(targetClusterId) = mutable.Set(target)
          nextClusterId += 1
        }

        // Merge clusters if source and target belong to different clusters
        for (sourceCluster <- sourceClusters; targetCluster <- targetClusters if sourceCluster != targetCluster) {
          if (clusterIdToActivities.contains(sourceCluster) && clusterIdToActivities.contains(targetCluster)) {
            // Merge activities
            val mergedActivities = clusterIdToActivities(sourceCluster) ++ clusterIdToActivities(targetCluster)
            clusterIdToActivities(sourceCluster) = mergedActivities
            clusterIdToActivities.remove(targetCluster)

            // Update activity mappings
            mergedActivities.foreach(activity => clusters(activity) = clusters(activity) + sourceCluster)

            println(s"Merged clusters: $sourceCluster and $targetCluster -> New Cluster: ${mergedActivities.mkString(", ")}")
          }
        }

        // If the relationship is significant, add the target to all source's clusters and vice versa
        if (isSignificant) {
          sourceClusters.foreach { clusterId =>
            if (clusterIdToActivities.contains(clusterId)) {
              clusterIdToActivities(clusterId) += target
              clusters(target) += clusterId
            }
          }
          targetClusters.foreach { clusterId =>
            if (clusterIdToActivities.contains(clusterId)) {
              clusterIdToActivities(clusterId) += source
              clusters(source) += clusterId
            }
          }
        }
      }
    }

    // Step 5: Print final clusters
    println("=== Final Clusters ===")
    clusterIdToActivities.zipWithIndex.foreach { case ((clusterId, activities), index) =>
      println(s"Cluster $index (ID: $clusterId):")
      activities.foreach(activity => println(s"  - $activity"))
    }

    println("=== Activity to Clusters Map ===")
    clusters.foreach { case (activity, clusterIds) =>
      println(s"Activity: $activity belongs to Clusters: ${clusterIds.mkString(", ")}")
    }

    // Stop Spark Session
    spark.stop()
  }
}
