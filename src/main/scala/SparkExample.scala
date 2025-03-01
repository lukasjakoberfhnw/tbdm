import org.apache.spark.sql.SparkSession

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    // Step 1: Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Spark SQL CSV Example")
      .master("local[*]") // Use all available cores for local mode
      .getOrCreate()

    // Step 2: Read the CSV file into a DataFrame
    val csvFilePath = "/tmp/data/sorted_logfile.csv" // Replace with the path to your CSV file
    val df = spark.read
      .option("header", "true") // Use "true" if your CSV has a header row
      .option("inferSchema", "true") // Automatically infer the schema
      .csv(csvFilePath)

    // Step 3: Show the content of the DataFrame
    println("DataFrame content:")
    df.show()

    /* 
    // Step 4: Print the schema of the DataFrame
    println("DataFrame schema:")
    df.printSchema()

    // Step 5: Perform some transformations and actions
    // Example: Select specific columns
    println("Select specific columns:")
    df.select("Column1", "Column2").show() // Replace Column1, Column2 with actual column names

    // Example: Filter rows based on a condition
    println("Filter rows where Column1 > 100:")
    df.filter("Column1 > 100").show() // Replace Column1 with the appropriate column name

    // Example: Group by a column and count
    println("Group by Column1 and count:")
    df.groupBy("Column1").count().show() // Replace Column1 with the appropriate column name
    */ 

    // Step 6: Stop the SparkSession
    spark.stop()
  }
}
