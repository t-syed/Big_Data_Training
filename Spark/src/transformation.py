
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, desc
from pyspark.sql.functions import sum, when, isnan

# Initialize SparkSession
spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# Path to CSV file on EC2's local filesystem
# csv_path = "/path/to/your/file.csv"

# Load CSV into DataFrame
#df = spark.read.csv(csv_path, header=True, inferSchema=True)

df_employee = spark.read.csv("/tmp/US_UK_05052025/syed/datasets/employees.csv", header=True, inferSchema=True)
df_department = spark.read.csv("/tmp/US_UK_05052025/syed/datasets/departments.csv", header=True, inferSchema=True)

num_rows = df_employee.count()
num_cols = len(df_employee.columns)

print(f"Shape: ({num_rows}, {num_cols})")


# Get summary statistics (mean, median, mode, std) of numerical columns.


column = "Salary"
mean_val = df_employee.select(mean(col(column))).first()[0]
std_val = df_employee.select(stddev(col(column))).first()[0]
median_val = df_employee.approxQuantile(column, [0.5], 0.01)[0]  # 1% relative error
mode_val = df_employee.groupBy(column).count().orderBy(desc("count")).first()[0]

# Print results
print(f"📊 Statistics for column '{column}':")
print(f"Mean (Average): {mean_val}")
print(f"Median: {median_val}")
print(f"Mode: {mode_val}")
print(f"Standard Deviation: {std_val}")


# alternative this comman can be used
#df_employee.describe("Salary").show()


#to print the data types of the fields in a table
df_employee.printSchema()

# to find missing values in columns

condition = None
for c in df_employee.columns:
    cond = col(c).isNull() | isnan(c)
    condition = cond if condition is None else condition | cond

df_employee.filter(condition).show()

# Rename a column in the DataFrame.

df_employee = df_employee.withColumnRenamed("EmployeeID", "EmpID")


# Filter the DataFrame to get rows where a column value is greater than a certain number.


df_filtered = df_employee.filter(col("Salary") > 60000)
df_filtered.show()

# Select specific columns from the DataFrame.

df_employee.select("EmpID", "Salary").Show()

# drop a specific column in a table
df_dropped = df_projects.drop("ProjectName")
df_dropped.show()

# Apply a transformation to a column (e.g., multiply each value by 2).,

df_transformed = df_employee.withColumn("Salary", col("Salary") * 2)
df_transformed.show()

# Add a new column based on an operation on existing columns

df_new = df_employee.withColumn("NewSalary", col("Salary") *2)
df_new.show()

# Group the data by a categorical column and get the mean of each group.

from pyspark.sql.functions import mean
df_grouped = df_employee.groupBy("Department").agg(mean("Salary").alias("Average_Salary"))
df_grouped.show()

# Sort the DataFrame based on a specific column.

df_sorted = df_employee.orderBy("Name")
df_sorted.show()

# Merge two DataFrames on a common column.

df_merged = df_employee.join(df_projects, on="EmployeeID", how="inner")
df_merged.show()

# Join two DataFrames using an index.

joined_df = df_employee.join(df_projects, df_employee.EmployeeID == df_projects.EmployeeID, "inner")
joined_df.show()




