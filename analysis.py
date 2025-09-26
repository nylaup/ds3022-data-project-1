import duckdb
import logging


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analyze_data():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        max_trip = con.execute(f"""
            -- Calculate largest carbon producing trip
            SELECT * FROM yellow_tripdata 
                WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM yellow_tripdata);
        """)
        max = max_trip.fetchone()[0]
        logger.info(f"The largest carbon producing trip of the year was {max}")

        max_hours = con.execute(f"""
            -- Most carbon heavy and light hours
        """)
        max = max_hours.fetchone()[0]
        logger.info(f"")

        max_weeks = con.execute(f"""
            -- Most carbon heavy and light weeks
        """)
        max = max_weeks.fetchone()[0]
        logger.info(f"")


        max_month = con.execute(f"""
            -- Most carbon heavy and light months
        """)
        max = max_month.fetchone()[0]
        logger.info(f"")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    #Time series plot
    
if __name__ == "__main__":
    analyze_data()