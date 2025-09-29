{{ config(materialized='table')}}

ALTER TABLE yellow_tripdata
ADD COLUMN avg_mph DOUBLE;
                    
UPDATE yellow_tripdata
SET avg_mph = (trip_distance * 3600) / trip_duration;