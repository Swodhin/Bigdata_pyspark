import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

def create_spark_session():
    """Initialize a Spark session"""
    return SparkSession.builder \
        .appName("Netflix ETL Transform") \
        .getOrCreate()

def load_and_clean(spark, input_dir, output_dir):
    """Stage 1: Load Netflix data, clean it, and save parquet."""

    # Define schema for Netflix dataset
    netflix_schema = T.StructType([
        T.StructField("show_id", T.StringType(), False),
        T.StructField("type", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("director", T.StringType(), True),
        T.StructField("cast", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("date_added", T.StringType(), True),
        T.StructField("release_year", T.IntegerType(), True),
        T.StructField("rating", T.StringType(), True),
        T.StructField("duration", T.StringType(), True),
        T.StructField("listed_in", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
    ])

    # Read the dataset
    df = spark.read.schema(netflix_schema).csv(
        os.path.join(input_dir, "netflix_titles.csv"), header=True
    )

    # Drop duplicates & null show_id
    df = df.dropDuplicates(["show_id"]).filter(F.col("show_id").isNotNull())

    # Parse date_added into proper DATE type
    df = df.withColumn("date_added", F.to_date(F.col("date_added"), "MMMM d, yyyy"))

    # Extract duration_value and duration_unit from duration column
    df = df.withColumn("duration_value", F.regexp_extract("duration", r"(\d+)", 1).cast(T.IntegerType())) \
           .withColumn("duration_unit", F.regexp_extract("duration", r"([A-Za-z]+)", 1))

    # Save cleaned data
    df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "netflix_shows"))
    print("Stage 1: Cleaned data saved")

    return df

def create_master_table(output_dir, df):
    """Stage 2: Create master Netflix table (already a flat table)."""

    master_df = df.select(
        "show_id", "type", "title", "director", "cast", "country",
        "date_added", "release_year", "rating", "duration",
        "duration_value", "duration_unit", "listed_in", "description"
    )

    master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "netflix_shows"))
    print("Stage 2: Master table saved")

def create_query_tables(output_dir, df):
    """Stage 3: Create query-optimized tables for genres and cast."""

    # Explode genres
    genres_df = df.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",\\s*"))) \
                  .select("show_id", "genre")
    genres_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "genres"))

    # Explode cast
    cast_df = df.filter(F.col("cast").isNotNull()) \
                .withColumn("actor", F.explode(F.split(F.col("cast"), ",\\s*"))) \
                .select("show_id", "actor")
    cast_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "cast"))

    print("Stage 3: Query-optimized tables saved")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python transform.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    df = load_and_clean(spark, input_dir, output_dir)
    create_master_table(output_dir, df)
    create_query_tables(output_dir, df)

    print("Transformation pipeline completed.")
