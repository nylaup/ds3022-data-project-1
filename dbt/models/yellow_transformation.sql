{{ config(materialized='external', location='models/green_transformed.csv',format='csv')}}

SELECT 
    t.trip_distance, t.pickup_datetime, t.dropoff_datetime, t.passenger_count,
    (t.trip_distance * e.co2_grams_per_mile / 1000) AS trip_co2,
    (t.trip_distance * 3600) / date_diff('seconds', t.dropoff_datetime, t.pickup_datetime) AS avg_mph,
    MONTH(t.pickup_datetime) AS month,
    WEEK(t.pickup_datetime) AS week_of_year,
    HOUR(t.pickup_datetime) AS hour,
    YEAR(t.pickup_datetime) AS year,
    CASE DAYOFWEEK(t.pickup_datetime)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week

FROM yellow_tripdata t
JOIN emissions_lookup e 
    ON e.vehicle_type = 'yellow'

{{log("Transformations complete for yellow table", info=True)}}