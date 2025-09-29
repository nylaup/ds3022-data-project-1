{{ config(materialized='table')}}

ALTER TABLE yellow_tripdata
ADD COLUMN month INTEGER,
ADD COLUMN week_of_year INTEGER,
ADD COLUMN day_of_week TEXT,
ADD COLUMN hour INT;
            
UPDATE yellow_tripdata;
SET month = MONTH(pickup_datetime),
    week_of_year = WEEK(pickup_datetime),
    hour = HOUR(pickup_datetime),
    day_of_week = CASE DAYOFWEEK(pickup_datetime)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END;