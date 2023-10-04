from pyspark.sql import SparkSession
from spark_read_postgres import arrival_df, departure_df
import pyspark.sql.functions as F 
from pyspark.sql.types import IntegerType   
from pyspark.sql.window import Window

#changing columns to integer format
def integer_format(df):
    numeric_columns = ["Arrival_terminal", "Departure_terminal"]

    for column in numeric_columns:
        df = df.withColumn(column, F.col(column).cast(IntegerType()))

    return df

#changing columns to time format
def time_format(df):
    time_columns = ["Departure_time_scheduled", "Departure_time_estimated", "Arrival_time_scheduled", "Arrival_time_estimated"]

    for column in time_columns:
        df = df.withColumn(column, F.to_timestamp(F.col(column), "HH:mm z"))
        df = df.withColumn(column, F.date_format(F.col(column), "HH:mm z"))

    return df

#changing columns to date format
def date_format(df):
    date_columns = ["Date_wise", "Departure_Date", "Arrival_Date"]

    for column in date_columns:
        df = df.withColumn(column, F.to_date(F.col(column), "dd-MM-yyyy"))
    
    return df

#changing column names to lower case
def lower_case(df):
    df = df.toDF(*[column.lower() for column in df.columns])

    return df

#correcting spelling mistakes in column names
def correcting_column_names(df):
    df = df.withColumnRenamed("Arrivial_time_estimated", "Arrival_time_estimated")

    return df

#function to transform and clean flight_delay_by_time and flight_time columns
def clean_time_columns(df):

    df = df.withColumn("delay_hours", F.regexp_extract("flight_delay_by_time", "(\\d+)h", 1))
    df = df.withColumn("delay_minutes", F.regexp_extract("flight_delay_by_time", "(\\d+)m", 1))
    df = df.withColumn("flight_hours", F.regexp_extract("flight_time", "(\\d+)h", 1))
    df = df.withColumn("flight_minutes", F.regexp_extract("flight_time", "(\\d+)m", 1))

    df = df.withColumn("delay_hours", F.col("delay_hours").cast(IntegerType()))
    df = df.withColumn("delay_minutes", F.col("delay_minutes").cast(IntegerType()))
    df = df.withColumn("flight_hours", F.col("flight_hours").cast(IntegerType()))
    df = df.withColumn("flight_minutes", F.col("flight_minutes").cast(IntegerType()))

    df = df.withColumn("delay_hours", F.when(df["delay_hours"].isNotNull(), df["delay_hours"]).otherwise(0))
    df = df.withColumn("flight_hours", F.when(df["flight_hours"].isNotNull(), df["flight_hours"]).otherwise(0))

    df = df.withColumn("delay_minutes", F.when(df["delay_minutes"].isNotNull(), df["delay_minutes"]).otherwise(0))
    df = df.withColumn("flight_minutes", F.when(df["flight_minutes"].isNotNull(), df["flight_minutes"]).otherwise(0))

    df = df.withColumn("total_delay_minutes", df["delay_hours"]*60 + df["delay_minutes"])
    df = df.withColumn("total_flight_time_minutes", df["flight_hours"]*60 + df["flight_minutes"])

    columns_to_drop = ["delay_hours", "delay_minutes", "flight_hours", "flight_minutes", "flight_time", "flight_delay_by_time"]

    df = df.drop(*columns_to_drop)

    return df

#function to transform and clean flight_distance column
def flight_distance_transformation(df):
    df = df.withColumn("flight_distance_metres", F.regexp_extract("flight_distance", "(\\d+) mi", 1))
    df = df.withColumn("flight_distance_metres", F.col("flight_distance_metres").cast(IntegerType()))

    df = df.drop("flight_distance")

    df = df.dropna(subset=["flight_distance_metres"])

    return df

#function to replace null terminal values with the average of all the terminals 
def replace_null_terminal_values(df):
    average = df.select(F.avg("arrival_terminal")).collect()[0][0]
    df = df.fillna(round(average), subset=["arrival_terminal"])

    average = df.select(F.avg("departure_terminal")).collect()[0][0]
    df = df.fillna(round(average), subset=["departure_terminal"])

    return df

#fucntion to replace null flight time values with the average of all the flight time values
def replace_null_flight_time(df):
    average = df.selectExpr("mean(total_flight_time_minutes)").collect()[0][0]
    df = df.fillna(average, subset=["total_flight_time_minutes"])

    return df

#function to replace null aircraft equipment details with the most common details
def filling_null_aircraft_equipment(df):
    most_common_value = df.groupBy("aircraft_equipment_description").count().orderBy(F.col("count").desc()).first()[0]
    df = df.fillna(most_common_value, subset=["aircraft_equipment_description"])

    most_common_value = df.groupBy("aircraft_equipment_code").count().orderBy(F.col("count").desc()).first()[0]
    df = df.fillna(most_common_value, subset=["aircraft_equipment_code"])

    return df

#function to drop all the rows containing invalid timestamps
def drop_invalid_timestamp(df):
    df = df.dropna(subset=["departure_time_estimated"])
    df = df.dropna(subset=["departure_time_scheduled"])
    df = df.dropna(subset=["arrival_time_scheduled"])
    df = df.dropna(subset=["arrival_time_estimated"])

    return df

spark = SparkSession.builder \
    .appName("DealingWithNullValues") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

#for invalid timestamps
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

arrival_df = correcting_column_names(arrival_df)
departure_df = correcting_column_names(departure_df)

arrival_df = time_format(arrival_df)
departure_df = time_format(departure_df)

arrival_df = date_format(arrival_df)
departure_df = date_format(departure_df)

arrival_df = lower_case(arrival_df)
departure_df = lower_case(departure_df)

arrival_df = clean_time_columns(arrival_df)
departure_df = clean_time_columns(departure_df)

arrival_df = flight_distance_transformation(arrival_df)
departure_df = flight_distance_transformation(departure_df)

arrival_df = replace_null_terminal_values(arrival_df)
departure_df = replace_null_terminal_values(departure_df)

arrival_df = replace_null_flight_time(arrival_df)
departure_df = replace_null_flight_time(departure_df)

arrival_df = filling_null_aircraft_equipment(arrival_df)
departure_df = filling_null_aircraft_equipment(departure_df)

arrival_df = drop_invalid_timestamp(arrival_df)
departure_df = drop_invalid_timestamp(departure_df)

