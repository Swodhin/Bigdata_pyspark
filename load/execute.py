import sys
import os
import psycopg2
from pyspark.sql import SparkSession

def create_spark_session():
    """Initialize a Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("Load Netflix Dataset") \
        .config("spark.jars", "/path/to/postgresql-42.7.7.jar") \
        .getOrCreate()

def create_postgres_tables():
    """Create PostgreSQL netflix_shows table if it doesn't exist."""
    conn, cursor = None, None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS netflix_shows (
                show_id VARCHAR(50) PRIMARY KEY,
                type VARCHAR(20),
                title TEXT,
                director TEXT,
                cast TEXT,
                country TEXT,
                date_added DATE,
                release_year INT,
                rating VARCHAR(10),
                duration TEXT,
                duration_value INT,
                duration_unit VARCHAR(20),
                listed_in TEXT,
                description TEXT
            );
        """)

        conn.commit()
        print("PostgreSQL table netflix_shows created successfully.")

    except Exception as e:
        print(f"Error creating table: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir):
    """Load cleaned Netflix dataset from Parquet into PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    try:
        # Expecting the transformed Netflix dataset saved at stage2/netflix_shows
        parquet_path = os.path.join(input_dir, "stage2/netflix_shows")
        df = spark.read.parquet(parquet_path)

        df.write \
            .mode("overwrite") \
            .jdbc(jdbc_url, "netflix_shows", properties=connection_properties)

        print("Loaded netflix_shows to PostgreSQL successfully.")

    except Exception as e:
        print(f"Error loading netflix_shows: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load.py <input_directory>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    if not os.path.exists(input_dir):
        print(f"Error: Input directory {input_dir} does not exist.")
        sys.exit(1)
    
    spark = create_spark_session()
    create_postgres_tables()
    load_to_postgres(spark, input_dir)
    
    print(" Load stage completed")
