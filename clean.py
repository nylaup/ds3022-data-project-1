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

        for color in ["yellow", "green"]:
            con.execute(f"""
                -- Remove duplicate trips
                CREATE TABLE {color}_tripdata_clean AS
                SELECT DISTINCT * FROM {color}_tripdata;

                DROP TABLE {color}_tripdata;
                ALTER TABLE {color}_tripdata_clean RENAME TO {color}_tripdata;
            """)
            logger.info(f"Removed duplicate trips from {color} table")
            print(f"Removed duplicate trips from {color} table")

            con.execute(f"""
                -- Remove trips with 0 passengers 
                DELETE FROM {color}_tripdata WHERE passenger_count = 0;
            """)
            logger.info(f"Removed trips with no passengers from {color} table")
            print(f"Removed trips with no passengers {color} table")

            con.execute(f"""
                -- Remove trips 0 miles
                DELETE FROM {color}_tripdata WHERE trip_distance = 0;
            """)
            logger.info(f"Removed trips with 0 miles from {color} table")
            print(f"Removed trips with 0 miles from {color} table")

            con.execute(f"""
                -- Remove trips over 100 miles
                DELETE FROM {color}_tripdata WHERE trip_distance > 100;
            """)
            logger.info(f"Removed trips over 100 miles from {color} table")
            print(f"Removed trips over 100 miles {color} table")

            con.execute(f"""
                -- Remove trips over 24 hours
                DELETE FROM {color}_tripdata 
                WHERE date_diff('seconds', dropoff_datetime, pickup_datetime) > 86400;
            """)
            logger.info(f"Removed trips over 24 hours from {color} table")
            print(f"Removed trips over 24 hours {color} table")

            count = con.execute(f"""
                -- SQL goes here 
                SELECT COUNT(*) FROM {color}_tripdata;
            """).fetchone()[0]
            print(f"{count} records in {color} table after cleaning")
            logger.info(f"{count} records in {color} table after cleaning")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_data()