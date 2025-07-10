-- Query 13: Day-of-week performance analysis by airline and airport
-- Analyzes operational patterns by day of week for airline-airport combinations
-- demonstrating star schema joins with comprehensive performance metrics

WITH daily_performance AS (
    SELECT 
        a.airline,
        ap.airport,
        ap.city,
        ap.state,
        f.day_of_week,
        CASE 
            WHEN f.day_of_week = 1 THEN 'Monday'
            WHEN f.day_of_week = 2 THEN 'Tuesday'
            WHEN f.day_of_week = 3 THEN 'Wednesday'
            WHEN f.day_of_week = 4 THEN 'Thursday'
            WHEN f.day_of_week = 5 THEN 'Friday'
            WHEN f.day_of_week = 6 THEN 'Saturday'
            WHEN f.day_of_week = 7 THEN 'Sunday'
        END AS day_name,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        ROUND(AVG(f.taxi_out), 2) AS avg_taxi_out_time,
        ROUND(SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_rate_pct
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    JOIN 
        airports ap ON f.origin = ap.iata_code
    WHERE 
        f.year = 2020
        AND f.day_of_week IS NOT NULL
    GROUP BY 
        a.airline, ap.airport, ap.city, ap.state, f.day_of_week
    HAVING 
        COUNT(*) >= 20
),
performance_rankings AS (
    SELECT 
        dp.*,
        AVG(dp.avg_departure_delay) OVER (PARTITION BY dp.airline, dp.airport) AS airline_airport_avg_delay,
        RANK() OVER (PARTITION BY dp.day_of_week ORDER BY dp.avg_departure_delay ASC) AS daily_delay_rank
    FROM 
        daily_performance dp
),
weekend_vs_weekday AS (
    SELECT 
        pr.airline,
        pr.airport,
        pr.city,
        pr.state,
        AVG(CASE WHEN pr.day_of_week IN (6,7) THEN pr.avg_departure_delay END) AS weekend_avg_delay,
        AVG(CASE WHEN pr.day_of_week BETWEEN 1 AND 5 THEN pr.avg_departure_delay END) AS weekday_avg_delay,
        AVG(CASE WHEN pr.day_of_week IN (6,7) THEN pr.cancellation_rate_pct END) AS weekend_cancellation_rate,
        AVG(CASE WHEN pr.day_of_week BETWEEN 1 AND 5 THEN pr.cancellation_rate_pct END) AS weekday_cancellation_rate
    FROM 
        performance_rankings pr
    GROUP BY 
        pr.airline, pr.airport, pr.city, pr.state
)
SELECT 
    wvw.airline,
    wvw.airport,
    wvw.city,
    wvw.state,
    ROUND(wvw.weekday_avg_delay, 2) AS weekday_avg_delay,
    ROUND(wvw.weekend_avg_delay, 2) AS weekend_avg_delay,
    ROUND(wvw.weekend_avg_delay - wvw.weekday_avg_delay, 2) AS weekend_delay_difference,
    ROUND(wvw.weekday_cancellation_rate, 2) AS weekday_cancellation_rate,
    ROUND(wvw.weekend_cancellation_rate, 2) AS weekend_cancellation_rate,
    CASE 
        WHEN wvw.weekend_avg_delay > wvw.weekday_avg_delay THEN 'Worse on Weekends'
        WHEN wvw.weekend_avg_delay < wvw.weekday_avg_delay THEN 'Better on Weekends'
        ELSE 'Similar Performance'
    END AS weekend_performance
FROM 
    weekend_vs_weekday wvw
WHERE 
    wvw.weekday_avg_delay IS NOT NULL 
    AND wvw.weekend_avg_delay IS NOT NULL
ORDER BY 
    ABS(wvw.weekend_avg_delay - wvw.weekday_avg_delay) DESC;
