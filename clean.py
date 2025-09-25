import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_data():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute(f"""
            -- Remove duplicate trips
            CREATE TABLE yellow_tripdata_clean AS
            SELECT DISTINCT * FROM yellow_tripdata;

            DROP TABLE yellow_tripdata;
            ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata;
        """)
        logger.info("Removed duplicate trips")
        print("Removed duplicate trips")

        con.execute(f"""
            -- Remove trips with 0 passengers 
            DELETE FROM yellow_tripdata WHERE passenger_count = 0;
        """)
        logger.info("Removed trips with no passengers")
        print("Removed trips with no passengers")

        con.execute(f"""
            -- Remove trips 0 miles
            DELETE FROM yellow_tripdata WHERE trip_distance = 0;
        """)
        logger.info("Removed trips with 0 miles")
        print("Removed trips with 0 miles")

        con.execute(f"""
            -- Remove trips over 100 miles
            DELETE FROM yellow_tripdata WHERE trip_distance > 100;
        """)
        logger.info("Removed trips over 100 miles")
        print("Removed trips over 100 miles")

        con.execute(f"""
            -- Remove trips over 24 hours
            ALTER TABLE yellow_tripdata 
            ADD COLUMN trip_duration INTEGER;

            UPDATE yellow_tripdata 
            SET trip_duration = date_diff('seconds', dropoff_datetime, pickup_datetime);
                    
            DELETE FROM yellow_tripdata WHERE trip_duration > 86400;
        """)
        logger.info("Removed trips over 24 hours")
        print("Removed trips over 24 hours")

        count = con.execute(f"""
            -- SQL goes here 
            SELECT COUNT(*) FROM yellow_tripdata;
        """)
        yellow_count = count.fetchone()[0]
        print(f"{yellow_count} records in yellow table after cleaning")
        logger.info(f"{yellow_count} records in yellow table after cleaning")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_data()