-- This query identifies the top 5 airports with the highest number of delayed flights in 2020, 
-- along with the percentage of delayed flights.
SELECT 
    ap.airport AS airport_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN f.arr_delay > 0 THEN 1 ELSE 0 END) AS delayed_flights,
    ROUND(SUM(CASE WHEN f.arr_delay > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM 
    flights f
JOIN 
    airports ap ON f.dest = ap.iata_code
WHERE 
    f.year = 2020
GROUP BY 
    ap.airport
ORDER BY 
    delayed_flights DESC
LIMIT 5;