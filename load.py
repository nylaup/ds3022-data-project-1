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

        all_10 = False #If website didn't get overwhelmed, set all_10 to True
        if all_10 == True:
            years = range(15,25) #Would then iterate through all 10 years to load data
        else:
            years = 2024 #Just use 2024 data 

        for color in ['yellow', 'green']: #Load both colors 
            for year in years: #Iterate through years 
                con.execute(f"""
                    -- Drop if existing and create new table with only needed columns
                    DROP TABLE IF EXISTS {color}_tripdata;
                    CREATE TABLE {color}_tripdata(
                        trip_distance double,
                        pickup_datetime timestamp,
                        dropoff_datetime timestamp,
                        passenger_count integer);
                """)
                logger.info(f"Created new {color} table")

                #Pickup columns have different names, add conditional to use right name 
                if color == "yellow": 
                    pickup = "tpep_pickup_datetime"
                    dropoff = "tpep_dropoff_datetime"
                elif color == "green":
                    pickup = "lpep_pickup_datetime"
                    dropoff = "lpep_dropoff_datetime"

                for month in range(1,13): #Load all months by substituting month in url 
                    url = (f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_20{year}-{month:02d}.parquet")
                    con.execute(f"""
                    -- Insert data from each month into table from url
                    INSERT INTO {color}_tripdata
                        SELECT trip_distance, {pickup}, {dropoff}, passenger_count
                        FROM read_parquet('{url}');
                    """)
                    logger.info(f"Added month {month} data to {color} table")
                    time.sleep(60) #Sleep so it doesn't overwhelm website 

                count = con.execute(f"""
                    -- Count records in table 
                    SELECT COUNT(*) FROM {color}_tripdata;
                """)
                records_count = count.fetchone()[0]
                print(f"Number of records in {color} taxi data: {records_count}")
                logger.info(f"Number of records in {color} taxi data: {records_count}")

        #EMISSIONS TABLE
        con.execute(f"""
            -- Create emissions lookup table from csv 
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