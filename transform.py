import duckdb
import logging


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

def transform_data():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute(f"""
            -- Calculate CO2 per trip (kg)
            ALTER TABLE yellow_tripdata 
            ADD COLUMN trip_co2_kgs DOUBLE;
                    
            UPDATE yellow_tripdata
            SET trip_co2_kgs = yellow_tripdata.trip_distance * e.co2_grams_per_mile / 1000
                FROM emissions_lookup e
                WHERE e.vehicle_type = 'yellow_taxi'; 
        """)
        logger.info("Added column for emissions per trip")

        con.execute(f"""
            -- Calculate mph per trip
            ALTER TABLE yellow_tripdata
            ADD COLUMN avg_mph DOUBLE;
                    
            UPDATE yellow_tripdata
            SET avg_mph = (trip_distance * 3600) / trip_duration;
        """)
        logger.info("Added column for trip mph")

        con.execute(f"""
            -- Add columns
            ALTER TABLE yellow_tripdata
            ADD COLUMN month INTEGER,
            ADD COLUMN week_of_year INTEGER,
            ADD COLUMN day_of_week TEXT,
            ADD COLUMN hour INT;
            
            UPDATE yellow_tripdata;
            SET month = MONTH(pickup_datetime),
                week_of_year = WEEK(pickup_datetime),
                hour = HOUR(pickup_datetime),
                day_of_week = CASE DAYOFWEEK(pickup_datetime)
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                    WHEN 7 THEN 'Sunday'
                END;
        """)
        logger.info("Added all date time columns")
        print("Added all date time columns")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_data()