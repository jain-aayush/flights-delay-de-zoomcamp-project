{{ 
    config(
        materialized='table',
        partition_by={
            'field' : 'Date',
            'data_type' : 'date',
            'granularity' : 'year'
        }, 
        cluster_by=['origin_airport_name', 'dest_airport_name']
    )
}}

with airport_data as (
    select * from {{ source('development', 'airports') }}
), 

carrier_data as (
    select * from {{ source('development', 'carriers') }}
),

flights_data as (
    select * from {{ ref('dev_flights') }}
),

plane_data as (
    select * from {{ source('development', 'plane_data') }}
)

select 
    flights_data.date,
    coalesce(flights_data.DepTime,-1) as deptime_filled, 
    flights_data.CRSDepTime, 
    coalesce(flights_data.ArrTime,-1) as arrtime_filled, 
    flights_data.CRSArrTime, 
    flights_data.UniqueCarrier, 
    carrier_data.Description as carrier_description, 
    flights_data.FlightNum, 
    flights_data.TailNum, 
    plane_data.type as plane_type, 
    plane_data.manufacturer as plane_manufacturer,
    plane_data.engine_type as plane_engine_type, 
    plane_data.aircraft_type as plane_aircraft_type, 
    flights_data.ActualElapsedTime,
    flights_data.CRSElapsedTime,
    flights_data.AirTime, 
    coalesce(flights_data.ArrDelay, 0) as arrdelay_filled,
    coalesce(flights_data.DepDelay,0) as depdelay_filled, 
    flights_data.Distance, 
    flights_data.Origin, 
    origin_airport.airport as origin_airport_name,
    origin_airport.lat as origin_airport_latitude,
    origin_airport.long as origin_airport_longitude,
    origin_airport.city as origin_airport_city,
    flights_data.Dest, 
    dest_airport.airport as dest_airport_name,
    dest_airport.long as dest_airport_longitude,
    dest_airport.lat as dest_airport_latitude,
    dest_airport.city as dest_airport_city,
    flights_data.diversion_status,
    flights_data.Diverted, 
    flights_data.cancellation_status, 
    flights_data.Cancelled,
    flights_data.cancellation_code_description,
    flights_data.CarrierDelay, 
    flights_data.WeatherDelay, 
    flights_data.NASDelay, 
    flights_data.SecurityDelay, 
    flights_data.LateAircraftDelay
from flights_data
left join airport_data as origin_airport
on flights_data.Origin = origin_airport.iata
left join airport_data as dest_airport
on flights_data.Dest = dest_airport.iata
left join carrier_data
on flights_data.UniqueCarrier = carrier_data.Code
left join plane_data
on flights_data.TailNum = plane_data.tailnum