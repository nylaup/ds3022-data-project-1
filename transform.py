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
        logger.info("")

        con.execute(f"""
            -- Calculate mph per trip
        """)
        logger.info("")

        con.execute(f"""
            -- Calculate pickup hour
        """)
        logger.info("")

        con.execute(f"""
            -- Calculate trip day of the week
        """)
        logger.info("")

        con.execute(f"""
            -- Calculate week number 
        """)
        logger.info("")

        con.execute(f"""
            -- Calculate month
        """)
        logger.info("")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_data()