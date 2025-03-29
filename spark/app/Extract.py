from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName("Arsenal_ETL") \
        .config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def read_and_write_csv_to_postgres(spark, csv_path, db_table):
    """Read a CSV file into a DataFrame and write it to a PostgreSQL table."""
    df = spark.read.csv(csv_path, header=True)
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5442/mydatabase") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", db_table) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

def main():
    """Main function to execute the ETL process."""
    spark = create_spark_session()

    # Corrected file paths inside the container
    matches_csv_path = "/opt/airflow/data/matches.csv"
    goalkeepers_csv_path = "/opt/airflow/data/goalkeepers.csv"
    players_csv_path = "/opt/airflow/data/players.csv"

    # Database table names
    matches_db_table = "arsenalmatches"
    goalkeepers_db_table = "arsenalGK"
    players_db_table = "arsenalPlayers"

    # Process each CSV file
    read_and_write_csv_to_postgres(spark, matches_csv_path, matches_db_table)
    read_and_write_csv_to_postgres(spark, goalkeepers_csv_path, goalkeepers_db_table)
    read_and_write_csv_to_postgres(spark, players_csv_path, players_db_table)

if __name__ == "__main__":
    main()
