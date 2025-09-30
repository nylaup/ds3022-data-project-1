import duckdb
import logging

#DID TRANSFORMATION USING DBT IN DBT FOLDER 

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

        for color in ["yellow", "green"]: 
            con.execute(f"""
                -- Calculate CO2 per trip (kg)
                ALTER TABLE {color}_tripdata 
                ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE;
                        
                UPDATE {color}_tripdata
                SET trip_co2_kgs = {color}_tripdata.trip_distance * e.co2_grams_per_mile / 1000
                    FROM emissions_lookup e
                    WHERE e.vehicle_type = '{color}_taxi'; 
            """)
            logger.info(f"Calculated emissions per trip for {color} table")

            con.execute(f"""
                --- Calculate average trip mph
                ALTER TABLE {color}_tripdata
                ADD COLUMN IF NOT EXISTS avg_mph DOUBLE;
                        
                UPDATE {color}_tripdata
                SET avg_mph = (trip_distance * 3600) / date_diff('seconds', dropoff_datetime, pickup_datetime);
            """)
            logger.info(f"Calculated average trip mph for {color} table")

            con.execute(f"ALTER TABLE {color}_tripdata ADD COLUMN IF NOT EXISTS month INTEGER;")
            con.execute(f"ALTER TABLE {color}_tripdata ADD COLUMN IF NOT EXISTS week_of_year INTEGER;")
            con.execute(f"ALTER TABLE {color}_tripdata ADD COLUMN IF NOT EXISTS day_of_week TEXT;")
            con.execute(f"ALTER TABLE {color}_tripdata ADD COLUMN IF NOT EXISTS hour INT;")
            logger.info(f"Added datetime columns to {color} table")

            con.execute(f"""
                -- Set datetime columns
                UPDATE {color}_tripdata
                SET month = MONTH(pickup_datetime),
                    week_of_year = WEEK(pickup_datetime),
                    hour = HOUR(pickup_datetime),
                    day_of_week = CASE DAYOFWEEK(pickup_datetime)
                        WHEN 0 THEN 'Sunday'
                        WHEN 1 THEN 'Monday'
                        WHEN 2 THEN 'Tuesday'
                        WHEN 3 THEN 'Wednesday'
                        WHEN 4 THEN 'Thursday'
                        WHEN 5 THEN 'Friday'
                        WHEN 6 THEN 'Saturday'
                    END;
            """)
            logger.info(f"Calculated date time columns to {color} table")

            print(f"Transformations complete to {color} table")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_data()