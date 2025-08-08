import sys
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

def create_spark_session():
    """intialize a Spark session."""
    return SparkSession.builder \
        .appName("Load and Execute") \
        .config("spark.jars", "/Users/najibthapa1/Documents/College/bigdata/pyspark/venv/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.7.jar") \
        .getOrCreate()
def create_postgres_tables():
    """Create PostgresSQL tables  if they don't exist using psycopg2."""
    conn = None
    try:
        conn  = psycopg2.connect(
            dbname = "postgres",
            user = "postgres",
            password = "postgres",
            host = "localhost",
            port = "5432"
            )
        cursor = conn.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id VARCHAR(50),
                artist_name TEXT,
                followers FLOAT,
                genres TEXT,
                artist_popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT,
                related_ids TEXT[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
                id VARCHAR(50),
                related_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_track (
                id VARCHAR(50),
                artist_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS tracks_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                popularity INTEGER,
                duration_ms INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artists_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                followers FLOAT,
                popularity INTEGER
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        print("PostgreSQL tables created successfully.")

    except Exception as e:
        print(f"Error creating tables:{e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir):
    """Load Parquet files to PostgresSql."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded","recommendations_exploded"),
        ("stage3/artist_track","artist_track"),
        ("stage3/tracks_metadata","tracks_metadata"),
        ("stage3/artists_metadata","artists_metadata")
    ]

    for parquet_path, table_name in tables:
        try:
            df=spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if 'master' in parquet_path else "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(jdbc_url, table_name, properties=connection_properties)
            print(f"Loaded {table_name} to PostgresSQL")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python execute.py <input_directory>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    if not os.path.exists(input_dir):
        print(f"Error: Input directory {input_dir} does not exist.")
        sys.exit(1)
    
    spark = create_spark_session()
    create_postgres_tables()
    load_to_postgres(spark, input_dir)
    
    print("Load stage completed")
