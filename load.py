import duckdb
import os
import logging

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

        con.execute(f"""
            -- Drop if existing and create new table
            DROP TABLE IF EXISTS yellow_tripdata;
            CREATE TABLE yellow_tripdata(
                trip_distance double(100),
                pickup_datetime timestamp(100)
            );
        """)
        logger.info("Dropped table if exists")

        yellow_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{:02d}.parquet"
        for month in range(1,13):
            url = yellow_url.format(month)
            con.execute(f"""
            -- Insert data from each month
            INSERT INTO yellow_tripdata
                SELECT trip_distance, tpep_pickup_datetime 
                FROM read_parquet('{url}');
            """)
            logger.info(f"Added month {month} data to yellow table")

        count = con.execute(f"""
            -- SQL goes here 
            SELECT COUNT(*) FROM yellow_tripdata;
        """)
        print(f"Number of records in yellow taxi data: {count.fetchone()[0]}")
        logger.info(f"Number of records in yellow taxi data: {count.fetchone()[0]}")

        con.execute(f"""
            DROP TABLE IF EXISTS emissions_lookup;
            CREATE TABLE emisssions_lookup AS 
                SELECT * FROM read_csv('data/vehicle_emissions.csv');
        """)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()