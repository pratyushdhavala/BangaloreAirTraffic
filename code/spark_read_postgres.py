from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, date_format
from pyspark.sql.types import StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("ReadFromPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

arrival_df = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/blr_air_traffic") \
    .option("user", "root") \
    .option("password", "root") \
    .option("dbtable", "arrival_data") \
    .load()

departure_df = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/blr_air_traffic") \
    .option("user", "root") \
    .option("password", "root") \
    .option("dbtable", "departure_data") \
    .load() 

print("Successfully read data from postgres")

arrival_df = arrival_df.withColumnRenamed("Arrivial_time_estimated", "Arrival_time_estimated")
departure_df = departure_df.withColumnRenamed("Arrivial_time_estimated", "Arrival_time_estimated")

date_columns = ["Date_wise", "Departure_Date", "Arrival_Date"]

time_columns = ["Departure_time_scheduled", "Departure_time_estimated", "Arrival_time_scheduled", "Arrival_time_estimated"]

numeric_columns = ["Arrival_terminal", "Departure_terminal"]

for column in date_columns:
    arrival_df = arrival_df.withColumn(column, to_timestamp(col(column), "dd-MM-yyyy"))
    departure_df = departure_df.withColumn(column, to_timestamp(col(column), "dd-MM-yyyy"))

for column in time_columns:
    arrival_df = arrival_df.withColumn(column, to_timestamp(col(column), "HH:mm z"))
    departure_df = departure_df.withColumn(column, to_timestamp(col(column), "HH:mm z"))
    
for column in time_columns:
    arrival_df = arrival_df.withColumn(column, date_format(col(column), "HH:mm z"))
    departure_df = departure_df.withColumn(column, date_format(col(column), "HH:mm z"))

for column in numeric_columns:
    arrival_df = arrival_df.withColumn(column, col(column).cast(IntegerType()))
    departure_df = departure_df.withColumn(column, col(column).cast(IntegerType()))


arrival_df = arrival_df.toDF(*[column.lower() for column in arrival_df.columns])
departure_df = departure_df.toDF(*[column.lower() for column in departure_df.columns])

arrival_df = arrival_df.withColumn("arrival_date", to_date(col("arrival_date"), "dd-MM-yyyy"))
arrival_df = arrival_df.withColumn("departure_date", to_date(col("departure_date"), "dd-MM-yyyy"))

departure_df = departure_df.withColumn("arrival_date", to_date(col("arrival_date"), "dd-MM-yyyy"))
departure_df = departure_df.withColumn("departure_date", to_date(col("departure_date"), "dd-MM-yyyy"))
