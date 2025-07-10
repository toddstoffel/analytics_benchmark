-- Query 17: Time-based operational performance analysis
-- Analyzes flight performance patterns by hour of day and day of week combinations
-- demonstrating temporal analysis with complex aggregations and operational insights

WITH hourly_performance AS (
    SELECT 
        CASE 
            WHEN f.day_of_week = 1 THEN 'Monday'
            WHEN f.day_of_week = 2 THEN 'Tuesday'
            WHEN f.day_of_week = 3 THEN 'Wednesday'
            WHEN f.day_of_week = 4 THEN 'Thursday'
            WHEN f.day_of_week = 5 THEN 'Friday'
            WHEN f.day_of_week = 6 THEN 'Saturday'
            WHEN f.day_of_week = 7 THEN 'Sunday'
        END AS day_name,
        f.day_of_week,
        CAST(SUBSTRING(f.dep_time, 1, 2) AS SIGNED) AS departure_hour,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        ROUND(AVG(f.air_time), 2) AS avg_air_time,
        ROUND(AVG(f.taxi_out), 2) AS avg_taxi_out_time,
        ROUND(AVG(f.taxi_in), 2) AS avg_taxi_in_time,
        SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) AS cancelled_flights,
        SUM(CASE WHEN f.dep_delay > 15 THEN 1 ELSE 0 END) AS significantly_delayed_flights
    FROM 
        flights f
    WHERE 
        f.year = 2020
        AND f.dep_time IS NOT NULL
        AND f.dep_time != ''
        AND LENGTH(f.dep_time) >= 2
        AND f.air_time IS NOT NULL
        AND f.taxi_out IS NOT NULL
        AND f.taxi_in IS NOT NULL
    GROUP BY 
        f.day_of_week, departure_hour
    HAVING 
        COUNT(*) >= 100
),
performance_rankings AS (
    SELECT 
        hp.*,
        ROUND(hp.cancelled_flights * 100.0 / hp.total_flights, 2) AS cancellation_rate_pct,
        ROUND(hp.significantly_delayed_flights * 100.0 / hp.total_flights, 2) AS significant_delay_rate_pct,
        RANK() OVER (PARTITION BY hp.day_of_week ORDER BY hp.avg_departure_delay ASC) AS best_departure_hours,
        RANK() OVER (PARTITION BY hp.departure_hour ORDER BY hp.avg_departure_delay ASC) AS best_days_for_hour,
        CASE 
            WHEN hp.departure_hour BETWEEN 6 AND 11 THEN 'Morning (6-11)'
            WHEN hp.departure_hour BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
            WHEN hp.departure_hour BETWEEN 18 AND 23 THEN 'Evening (18-23)'
            ELSE 'Night/Early Morning (0-5)'
        END AS time_period
    FROM 
        hourly_performance hp
)
SELECT 
    day_name,
    time_period,
    departure_hour,
    total_flights,
    avg_departure_delay,
    avg_arrival_delay,
    avg_air_time,
    avg_taxi_out_time,
    avg_taxi_in_time,
    cancellation_rate_pct,
    significant_delay_rate_pct,
    best_departure_hours,
    best_days_for_hour
FROM 
    performance_rankings
WHERE 
    total_flights >= 500
ORDER BY 
    day_of_week, departure_hour;
