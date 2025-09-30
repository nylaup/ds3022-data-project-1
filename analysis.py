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

        for color in ["yellow", "green"]: #Do same analysis code on both tables 
            max_trip = con.execute(f"""
                -- Calculate largest carbon producing trip
                SELECT trip_co2_kgs, pickup_datetime FROM {color}_tripdata 
                WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM {color}_tripdata);
                """).fetchone()
            carbon, date = max_trip #Collect the co2 emissions and the date 
            logger.info(f"The largest carbon producing trip of the year for {color} was {carbon} kg on {date}")
            print(f"The largest carbon producing trip of the year for {color} was {carbon} kg on {date}")

            #Find out what the most and least carbon heavy of each of these units of time is 
            for time in ["hour", "day_of_week", "week_of_year", "month"]:
                max_hours = con.execute(f"""
                    -- Most carbon heavy time
                    SELECT {time} 
                    FROM {color}_tripdata
                    GROUP BY {time}
                    ORDER BY AVG(trip_co2_kgs) DESC
                    LIMIT 1;
                """).fetchone()[0]
                logger.info(f"The most carbon heavy {time} for {color} is {max_hours}")
                print(f"The most carbon heavy {time} for {color} is {max_hours}")

                min_hours = con.execute(f"""
                    -- Least carbon heavy time
                    SELECT {time}
                    FROM {color}_tripdata
                    GROUP BY {time}
                    ORDER BY AVG(trip_co2_kgs) ASC
                    LIMIT 1;
                """).fetchone()[0]
                logger.info(f"The least carbon heavy {time} for {color} is {min_hours}")
                print(f"The least carbon heavy {time} for {color} is {min_hours}")

        yellow_df = con.execute(f"""
            --- Calculate monthly total for Yellow for graph
            SELECT month, SUM(trip_co2_kgs) AS total_co2
            FROM yellow_tripdata
            GROUP BY month
            ORDER BY month;
        """).fetchdf()

        green_df = con.execute(f"""
            --- Calculate monthly total for Green for graph
            SELECT month, SUM(trip_co2_kgs) AS total_co2
            FROM green_tripdata
            GROUP BY month
            ORDER BY month;
        """).fetchdf()

        #Use plt.subplot to make two plots, both in one column 
        fig, axes = plt.subplots(2, 1, sharex=True)
        sns.lineplot(ax=axes[0], x="month", y="total_co2", data=yellow_df, color="yellow", label="Yellow Taxi")
        sns.lineplot(ax=axes[1], x="month", y="total_co2", data=green_df, color="green", label="Green Taxi")

        axes[0].set_title("CO2 totals per month for taxi types")
        axes[0].set_xlabel("Month")
        axes[0].set_ylabel("CO2 Emissions")
        axes[1].set_ylabel("CO2 Emissions")

        #plt.savefig('emissionsLineplot.png') #Save plot as png figure 
        logger.info("Successfully rendered the plot")
        print("Successfully rendered the plot")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    
if __name__ == "__main__":
    analyze_data()