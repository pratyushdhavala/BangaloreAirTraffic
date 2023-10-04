from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

# Sample data
data = [("A", 10), ("A", 20), ("A", 30), ("B", 5), ("B", 15), ("B", 25)]
columns = ["group", "value"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define a Window specification partitioned by 'group'
windowSpec = Window.partitionBy("group").orderBy("value")

# Example 1: Calculate the cumulative sum within each group
df = df.withColumn("cumulative_sum", F.sum("value").over(windowSpec))

# Example 2: Calculate the row number within each group
df = df.withColumn("row_number", F.row_number().over(windowSpec))

# Example 3: Calculate the difference between the current row's value and the previous row's value within each group
df = df.withColumn("value_difference", F.col("value") - F.lag("value").over(windowSpec))

# Show the DataFrame with the added columns
df.show()
