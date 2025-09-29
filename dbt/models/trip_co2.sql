{{ config(materialized='table')}}

ALTER TABLE yellow_tripdata 
ADD COLUMN trip_co2_kgs DOUBLE;
                    
UPDATE yellow_tripdata
SET trip_co2_kgs = yellow_tripdata.trip_distance * e.co2_grams_per_mile / 1000
    FROM emissions_lookup e
    WHERE e.vehicle_type = 'yellow_taxi'; 