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
            -- Calculate pickup hour
            ALTER TABLE yellow_tripdata
            ADD COLUMN hour INT;

            UPDATE yellow_tripdata
            SET hour = EXTRACT(HOUR FROM pickup_datetime);
        """)
        logger.info("Added column for pickup hour")

        con.execute(f"""
            -- Calculate trip day of the week
            ALTER TABLE yellow_tripdata
            ADD COLUMN day_of_week TEXT;
                    
            UPDATE yellow_tripdata
            SET day_of_week = EXTRACT(DAYNAME(dayofweek FROM pickup_datetime));
        """)
        logger.info("Added column for week of year")

        con.execute(f"""
            -- Calculate week number 
            ALTER TABLE yellow_tripdata
            ADD COLUMN week_of_year INTEGER;
                    
            UPDATE yellow_tripdata
            SET week_of_year = EXTRACT(WEEK FROM pickup_datetime);
        """)
        logger.info("Added column for week of year")

        con.execute(f"""
            -- Calculate month
            ALTER TABLE yellow_tripdata
            ADD COLUMN month INT;

            UPDATE yellow_tripdata
            SET month = EXTRACT(MONTH FROM pickup_datetime);
        """)
        logger.info("")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_data()