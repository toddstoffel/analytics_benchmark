-- Query 12: Seasonal airline performance analysis
-- Analyzes monthly airline performance patterns and seasonal rankings
-- demonstrating fact/dimension joins with advanced window functions
-- Optimized for memory efficiency while maintaining analytical value

WITH monthly_airline_performance AS (
    SELECT 
        a.airline,
        f.month,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        ROUND(SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_rate_pct,
        ROUND(SUM(CASE WHEN f.diverted > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS diversion_rate_pct
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.fl_date IS NOT NULL
        AND f.month BETWEEN 1 AND 12  -- Explicit range for better optimization
    GROUP BY 
        a.airline, f.month
    HAVING 
        COUNT(*) >= 100  -- Move filter to HAVING for better memory usage
)
SELECT 
    map.airline,
    map.month,
    CASE 
        WHEN map.month = 1 THEN 'January'
        WHEN map.month = 2 THEN 'February'
        WHEN map.month = 3 THEN 'March'
        WHEN map.month = 4 THEN 'April'
        WHEN map.month = 5 THEN 'May'
        WHEN map.month = 6 THEN 'June'
        WHEN map.month = 7 THEN 'July'
        WHEN map.month = 8 THEN 'August'
        WHEN map.month = 9 THEN 'September'
        WHEN map.month = 10 THEN 'October'
        WHEN map.month = 11 THEN 'November'
        WHEN map.month = 12 THEN 'December'
    END AS month_name,
    map.total_flights,
    map.avg_departure_delay,
    map.avg_arrival_delay,
    map.cancellation_rate_pct,
    map.diversion_rate_pct,
    RANK() OVER (PARTITION BY map.month ORDER BY map.avg_arrival_delay ASC) AS delay_rank_in_month,
    RANK() OVER (PARTITION BY map.month ORDER BY map.cancellation_rate_pct ASC) AS cancellation_rank_in_month,
    CASE 
        WHEN RANK() OVER (PARTITION BY map.month ORDER BY map.avg_arrival_delay ASC) <= 3 THEN 'Top Performer'
        WHEN RANK() OVER (PARTITION BY map.month ORDER BY map.avg_arrival_delay ASC) >= COUNT(*) OVER (PARTITION BY map.month) - 2 THEN 'Poor Performer'
        ELSE 'Average Performer'
    END AS performance_category
FROM 
    monthly_airline_performance map
ORDER BY 
    map.month, map.avg_arrival_delay;
