import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #YELLOW TAXI
        con.execute(f"""
            -- Drop if existing and create new table
            DROP TABLE IF EXISTS yellow_tripdata;
            CREATE TABLE yellow_tripdata(
                trip_distance double,
                pickup_datetime timestamp,
                dropoff_datetime timestamp,
                passenger_count integer);
        """)
        logger.info("Dropped yellow table if exists")

        yellow_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{:02d}.parquet"
        for month in range(1,13):
            url = yellow_url.format(month)
            con.execute(f"""
            -- Insert data from each month
            INSERT INTO yellow_tripdata
                SELECT trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
                FROM read_parquet('{url}');
            """)
            logger.info(f"Added month {month} data to yellow table")
            time.sleep(60)

        count = con.execute(f"""
            -- SQL goes here 
            SELECT COUNT(*) FROM yellow_tripdata;
        """)
        yellow_count = count.fetchone()[0]
        print(f"Number of records in yellow taxi data: {yellow_count}")
        logger.info(f"Number of records in yellow taxi data: {yellow_count}")

        #GREEN TAXI
        con.execute(f"""
            -- Drop if existing and create new table
            DROP TABLE IF EXISTS green_tripdata;
            CREATE TABLE green_tripdata(
                trip_distance double,
                pickup_datetime timestamp,
                dropoff_datetime timestamp,
                passenger_count integer);
        """)
        logger.info("Dropped green table if exists")

        green_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{:02d}.parquet"
        for month in range(1,13):
            url = green_url.format(month)
            con.execute(f"""
            -- Insert data from each month
            INSERT INTO green_tripdata
                SELECT trip_distance, lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count
                FROM read_parquet('{url}');
            """)
            logger.info(f"Added month {month} data to green table")
            time.sleep(60)

        count = con.execute(f"""
            -- SQL goes here 
            SELECT COUNT(*) FROM green_tripdata;
        """)
        green_count = count.fetchone()[0]
        print(f"Number of records in green taxi data: {green_count}")
        logger.info(f"Number of records in green taxi data: {green_count}")

        #EMISSIONS TABLE
        con.execute(f"""
            DROP TABLE IF EXISTS emissions_lookup;
            CREATE TABLE emissions_lookup AS 
                SELECT * FROM read_csv('data/vehicle_emissions.csv');
        """)
        logger.info("Created emissions lookup table")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()