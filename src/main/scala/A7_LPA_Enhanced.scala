import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object A7_LPA_Enhanced {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ActivityGraphLPA")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val csvPath = Paths.get("data", "sorted_logfile.csv").toString

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    val activitiesRDD: RDD[(VertexId, (String, Long))] = df.rdd.map(row => {
      val activity = row.getAs[String]("Activity")
      (activity.hashCode.toLong, (activity, activity.hashCode.toLong))
    }).distinct()

    val indexedRDD = df.rdd.zipWithIndex()
    val reversedIndexRDD = indexedRDD.map { case (row, index) => (index, row) }

    val consecutiveRows = reversedIndexRDD
      .join(reversedIndexRDD.map { case (index, row) => (index - 1, row) })
      .values

    val transitionCounts: RDD[((String, String), Int)] = consecutiveRows.map { case (currentRow, nextRow) =>
      val fromActivity = currentRow.getAs[String]("Activity")
      val toActivity = nextRow.getAs[String]("Activity")
      ((fromActivity, toActivity), 1)
    }.reduceByKey(_ + _)

    val totalOutgoingCounts: RDD[(String, Int)] = transitionCounts
      .map { case ((from, _), count) => (from, count) }
      .reduceByKey(_ + _)

    val transitionProbabilities: RDD[((String, String), Double)] = transitionCounts
      .map { case ((from, to), count) => (from, (to, count)) }
      .join(totalOutgoingCounts)
      .map { case (from, ((to, count), totalCount)) =>
        ((from, to), count.toDouble / totalCount.toDouble)
      }

    val edges: RDD[Edge[Double]] = transitionProbabilities
      .filter { case ((from, to), probability) => probability > 0 }
      .map { case ((from, to), probability) =>
        Edge(from.hashCode.toLong, to.hashCode.toLong, probability)
      }

    val graph = Graph(activitiesRDD.mapValues(_._2), edges)

    val maxIterations = 10
    val lpaGraph = graph.pregel(Long.MaxValue, maxIterations)(
      (id, attr, newAttr) => math.min(attr, newAttr),
      triplet => Iterator((triplet.dstId, triplet.srcAttr)),
      (a, b) => math.min(a, b)
    )

    val communityAssignments = lpaGraph.vertices
      .join(activitiesRDD)
      .map { case (id, (community, (activityName, _))) => (community, activityName) }
      .groupByKey()
      .mapValues(_.toList)

    val results = communityAssignments.collect().map { case (community, activities) =>
      s"$community:${activities.mkString(",")}"
    }.mkString("\n")

    val outputPath = "A7_lpa_enhanced.txt"
    Files.write(Paths.get(outputPath), results.getBytes(StandardCharsets.UTF_8))

    println(s"Results saved to $outputPath")
    spark.stop()
  }
}
