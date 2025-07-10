-- Query 16: Aircraft utilization and efficiency analysis
-- Analyzes aircraft tail numbers by utilization metrics, flight frequency, and operational efficiency
-- demonstrating fact table analysis with advanced aggregations and performance metrics

WITH aircraft_utilization AS (
    SELECT 
        f.tail_num,
        a.airline,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT f.fl_date) AS active_days,
        ROUND(COUNT(*) / COUNT(DISTINCT f.fl_date), 2) AS avg_flights_per_day,
        SUM(f.distance) AS total_distance_miles,
        ROUND(AVG(f.distance), 2) AS avg_flight_distance,
        SUM(f.air_time) AS total_air_time_minutes,
        ROUND(AVG(f.air_time), 2) AS avg_air_time,
        SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) AS cancelled_flights,
        SUM(CASE WHEN f.diverted > 0 THEN 1 ELSE 0 END) AS diverted_flights,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.year = 2020
        AND f.tail_num IS NOT NULL
        AND f.tail_num != ''
        AND f.air_time IS NOT NULL
        AND f.distance IS NOT NULL
    GROUP BY 
        f.tail_num, a.airline
    HAVING 
        COUNT(*) >= 100
),
efficiency_metrics AS (
    SELECT 
        au.*,
        ROUND(au.cancelled_flights * 100.0 / au.total_flights, 2) AS cancellation_rate_pct,
        ROUND(au.diverted_flights * 100.0 / au.total_flights, 2) AS diversion_rate_pct,
        ROUND(au.total_air_time_minutes / 60.0, 2) AS total_air_time_hours,
        ROUND(au.total_distance_miles / NULLIF(au.total_air_time_minutes, 0) * 60, 2) AS avg_speed_mph,
        RANK() OVER (PARTITION BY au.airline ORDER BY au.total_flights DESC) AS utilization_rank_in_airline
    FROM 
        aircraft_utilization au
)
SELECT 
    airline,
    tail_num,
    total_flights,
    active_days,
    avg_flights_per_day,
    total_distance_miles,
    avg_flight_distance,
    total_air_time_hours,
    avg_air_time,
    avg_speed_mph,
    avg_departure_delay,
    avg_arrival_delay,
    cancellation_rate_pct,
    diversion_rate_pct,
    utilization_rank_in_airline
FROM 
    efficiency_metrics
WHERE 
    utilization_rank_in_airline <= 5
ORDER BY 
    airline, utilization_rank_in_airline;
