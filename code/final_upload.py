from pyspark.sql import SparkSession
from transform import arrival_df,departure_df

spark = SparkSession.builder \
    .appName("FinalUpload") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/blr_air_traffic"

arrival_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "arrival_data_cleaned") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

departure_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "departure_data_cleaned") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Successfully uploaded data to postgres")