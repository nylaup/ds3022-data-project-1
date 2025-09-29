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

        for color in ['yellow', 'green']:
            con.execute(f"""
                -- Drop if existing and create new table
                DROP TABLE IF EXISTS {color}_tripdata;
                CREATE TABLE {color}_tripdata(
                    trip_distance double,
                    pickup_datetime timestamp,
                    dropoff_datetime timestamp,
                    passenger_count integer);
            """)
            logger.info(f"Created new {color} table")

            if color == "yellow":
                pickup = "tpep_pickup_datetime"
                dropoff = "tpep_dropoff_datetime"
            elif color == "green":
                pickup = "lpep_pickup_datetime"
                dropoff = "lpep_dropoff_datetime"

            for month in range(1,13):
                url = (f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_2024-{month:02d}.parquet")
                con.execute(f"""
                -- Insert data from each month
                INSERT INTO {color}_tripdata
                    SELECT trip_distance, {pickup}, {dropoff}, passenger_count
                    FROM read_parquet('{url}');
                """)
                logger.info(f"Added month {month} data to {color} table")
                time.sleep(60)

            count = con.execute(f"""
                -- Count records
                SELECT COUNT(*) FROM {color}_tripdata;
            """)
            records_count = count.fetchone()[0]
            print(f"Number of records in {color} taxi data: {records_count}")
            logger.info(f"Number of records in {color} taxi data: {records_count}")

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