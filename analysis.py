import duckdb
import logging
import matplotlib.pyplot as plt
import seaborn as sns 


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

        def carbon_heavy(color, time):
            max_trip = con.execute(f"""
                -- Calculate largest carbon producing trip
                SELECT * FROM {color}_tripdata 
                WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM {color}_tripdata);
                """).fetchone()
            logger.info(f"The largest carbon producing trip of the year for {color} was {max_trip}")

            max_hours = con.execute(f"""
                -- Most carbon heavy time
                SELECT {time} 
                FROM {color}_tripdata
                GROUP BY {time}
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()
            logger.info(f"The most carbon heavy {time} for {color} is {max_hours}")

            min_hours = con.execute(f"""
                -- Least carbon heavy time
                SELECT {time}
                FROM {color}_tripdata
                GROUP BY {time}
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()
            logger.info(f"The least carbon heavy {time} for {color} is {min_hours}")

        colors = ["yellow", "green"]
        times = ["hour", "day_of_week", "week_of_year", "month"]
        for color in colors:
            for time in times:
                carbon_heavy(color, time)

        yellow_df = con.execute(f"""
            --- Calculate monthly total for Yellow
            SELECT month, SUM(trip_co2_kgs) AS total_co2
            FROM yellow_tripdata
            GROUP BY month
            ORDER BY month;
        """).fetchdf()

        green_df = con.execute(f"""
            --- Calculate monthly total for Green
            SELECT month, SUM(trip_co2_kgs) AS total_co2
            FROM green_tripdata
            GROUP BY month
            ORDER BY month;
        """).fetchdf()

        ax=sns.lineplot(x="month", y="total_co2", data=yellow_df, label="Yellow Taxi")
        sns.lineplot(x="month", y="total_co2", data=green_df, label="Green Taxi", ax=ax)
        plt.title("CO2 totals per month for taxi types")
        plt.xlabel("Month")
        plt.ylabel("CO2 Emissions")
        plt.show()

        figure = ax.get_figure()
        figure.savefig('emissionsLineplot.png')

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    
if __name__ == "__main__":
    analyze_data()