import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.util.control.Breaks._

object ActivityNeighbourhood2 {
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
      val neighbors = (-1 to 2).filter(_ != 0).map(offset => (index + offset, activity)) // -2 to 2 was kinda good -- -1 to 3 is quite good
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
    val clusters = mutable.Map[String, Set[String]]()
    relationshipsCollected.flatMap { case (relationship, _) =>
      val Array(sourceActivity, targetActivity) = relationship.split("/")
      Seq(sourceActivity, targetActivity)
    }.distinct.foreach(activity => clusters(activity) = Set(activity))

    // Create a priority queue for relationships (sorted by count in descending order)
    val relationshipQueue = mutable.PriorityQueue[(String, String, Int)]()(
      Ordering.by(_._3) // Sort by count (highest first)
    )
    relationshipQueue ++= relationshipsCollected.map { case (relationship, count) =>
      val Array(sourceActivity, targetActivity) = relationship.split("/")
      (sourceActivity, targetActivity, count)
    }

    // Iteratively merge clusters with size and count constraints
    breakable {
      while (clusters.size > 15 && relationshipQueue.nonEmpty) {
        val (source, target, count) = relationshipQueue.dequeue()

        // Stop if the count is less than 2
        if (count < 2) { // default with good results: 2
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
          if (sourceActivities.size + targetActivities.size > 6) { // 4 was default so far to restrict very big clusters
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
