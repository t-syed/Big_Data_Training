

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("CSV Loader")
  .master("local[*]") // or your cluster master
  .getOrCreate()

// Load CSV file into DataFrame
// val df_employees = spark.read.option("header", "true").option("inferSchema", "true").csv("/tmp/US_UK_05052025/syed/datasets/employees.csv")



val df_employees = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/US_UK_05052025/syed/datasets/employees.csv")

val df_departments = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/US_UK_05052025/syed/datasets/departments.csv")

val df_projects = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/tmp/US_UK_05052025/syed/datasets/projects.csv")

df_employees.show()
df_departments.show()
df_projects.show()

// Get the shape of the DataFrame (rows, columns)

val numRows = df_employees.count()
val numCols = df_employees.columns.length
println(s"Shape: ($numRows, $numCols)")


// Get summary statistics (mean, median, mode, std) of numerical columns.

df_employees.describe().show()

// Get the data types of each column.

df_employees.printSchema()

// Check for missing values in each column
	
	
val df_missing = df_employees.columns.map { colName =>
  val nullCount = df_employees.filter(col(colName).isNull || isnan(col(colName))).count()
  (colName, nullCount)
}

// Print results
df_missing.foreach { case (colName, count) =>
  println(s"$colName: $count missing values")
}

// Rename a column in the DataFrame.

val dfRenamed = df_projects.withColumnRenamed("ProjectID", "ProjID")

dfRenamed.show()