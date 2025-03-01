name := "GraphX Example"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-graphx" % "3.5.4",
  "org.apache.spark" %% "spark-sql" % "3.5.4" // Add this line for Spark SQL
)