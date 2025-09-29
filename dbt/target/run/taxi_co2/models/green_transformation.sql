
    create or replace view "emissions"."main"."green_transformation__dbt_int" as (
      select * from read_csv('/home/nylup/DS3022/ds3022-data-project-1/emissions.duckdb', auto_detect=True)
      -- if relation is empty, filter by all columns having null values
      
        where 1 AND "trip_distance" is not NULL AND "pickup_datetime" is not NULL AND "dropoff_datetime" is not NULL AND "passenger_count" is not NULL AND "trip_co2" is not NULL AND "avg_mph" is not NULL AND "month" is not NULL AND "week_of_year" is not NULL AND "hour" is not NULL AND "year" is not NULL AND "day_of_week" is not NULL
    );
    