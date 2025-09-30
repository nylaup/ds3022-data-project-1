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

        #Clean both data tables using same code by string replacing the color in SQL queries 
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
                DELETE FROM {color}_tripdata WHERE passenger_count = 0
            """)
            min_pass = con.execute(f"""
                -- Check what the minimum passenger count is now 
                SELECT min(passenger_count) FROM {color}_tripdata;
            """).fetchone()[0]
            logger.info(f"Removed trips with no passengers from {color} table, now min passengers is {min_pass}")
            print(f"Removed trips with no passengers {color} table, now min passengers is {min_pass}")

            con.execute(f"""
                -- Remove trips 0 miles
                DELETE FROM {color}_tripdata WHERE trip_distance = 0
            """)
            min_miles = con.execute(f"""
                -- Check what the minimum miles are after removal 
                SELECT min(trip_distance) FROM {color}_tripdata;
            """).fetchone()[0]
            logger.info(f"Removed trips with 0 miles from {color} table, now min distance is {min_miles}")
            print(f"Removed trips with 0 miles from {color} table, now min distance is {min_miles}")

            con.execute(f"""
                -- Remove trips over 100 miles
                DELETE FROM {color}_tripdata WHERE trip_distance > 100
            """)
            max_distance = con.execute(f"""
                -- Check what max trip distance is now
                SELECT max(trip_distance) FROM {color}_tripdata;
            """).fetchone()[0]
            logger.info(f"Removed trips over 100 miles from {color} table, now max distance is {max_distance}")
            print(f"Removed trips over 100 miles {color} table, now max distance is {max_distance}")

            con.execute(f"""
                -- Remove trips over 24 hours
                DELETE FROM {color}_tripdata 
                WHERE date_diff('seconds', dropoff_datetime, pickup_datetime) > 86400
            """)
            max_seconds = con.execute(f"""
                -- Check what max trip time is now 
                SELECT max(date_diff('seconds', dropoff_datetime, pickup_datetime)) FROM {color}_tripdata
            """).fetchone()[0]
            max_seconds = max_seconds / 3600 #convert back to hours
            logger.info(f"Removed trips over 24 hours from {color} table, now max time is {max_seconds}")
            print(f"Removed trips over 24 hours {color} table, now max time is {max_seconds}")

            count = con.execute(f"""
                -- Count total records after cleaning to ensure it has removed some  
                SELECT COUNT(*) FROM {color}_tripdata;
            """).fetchone()[0]
            print(f"{count} records in {color} table after cleaning")
            logger.info(f"{count} records in {color} table after cleaning")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_data()