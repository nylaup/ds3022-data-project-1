

SELECT 
    t.trip_distance, t.pickup_datetime, t.dropoff_datetime, t.passenger_count,
    -- Calculate co2 output by multiplying trip distance by grams per mile, then divide to convert to kg
    (t.trip_distance * e.co2_grams_per_mile / 1000) AS trip_co2,
    -- Calculate avg mph by dividing distance by trip time, then multiplied by 3600 to convert to hours
    (t.trip_distance * 3600) / date_diff('seconds', t.pickup_datetime, t.dropoff_datetime) AS avg_mph,

    -- Extract month, week, hour, and year from datetime
    MONTH(t.pickup_datetime) AS month,
    WEEK(t.pickup_datetime) AS week_of_year,
    HOUR(t.pickup_datetime) AS hour,
    YEAR(t.pickup_datetime) AS year,
    -- Assign day of week based on DAYOFWEEK number 
    CASE DAYOFWEEK(t.pickup_datetime)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week

FROM "emissions"."main"."green_tripdata" t
JOIN "emissions"."main"."emissions_lookup" e
    ON e.vehicle_type = 'yellow_taxi' --Select emissions that matches with taxi color 

