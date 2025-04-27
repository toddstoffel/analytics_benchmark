#!/usr/bin/env bash
curl -v --location-trusted -u root \
    -H "column_separator:," \
    -H "columns:iata_code,airline" \
    -H "Expect: 100-continue" \
    -T bts.airlines.csv \
    -XPUT http://127.0.0.1:8040/api/bts/airlines/_stream_load

curl -v --location-trusted -u root \
    -H "column_separator:," \
    -H "columns:iata_code,airport,city,state,country,latitude,longitude" \
    -H "Expect: 100-continue" \
    -T bts.airports.csv \
    -XPUT http://127.0.0.1:8040/api/bts/airports/_stream_load

curl -v --location-trusted -u root \
    -H "column_separator:," \
    -H "trim_double_quotes:true" \
    -H "columns: year,month,day,day_of_week,fl_date,carrier,tail_num,fl_num,origin,dest,crs_dep_time,dep_time,dep_delay,taxi_out,wheels_off,wheels_on,taxi_in,crs_arr_time,arr_time,arr_delay,cancelled,cancellation_code,diverted,crs_elapsed_time,actual_elapsed_time,air_time,distance,carrier_delay,weather_delay,nas_delay,security_delay,late_aircraft_delay" \
    -H "enclose:\"" \
    -H "Expect: 100-continue" \
    -T bts.flights.csv \
    -XPUT http://127.0.0.1:8040/api/bts/flights/_stream_load
