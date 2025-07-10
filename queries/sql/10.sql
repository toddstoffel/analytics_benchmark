-- Query 10: Top 5 airports with highest delayed flights in 2020
-- Identifies airports with the most delayed flights and calculates delay percentages
-- demonstrating fact/dimension joins and ranking analysis

WITH airport_delays AS (
    SELECT 
        ap.airport,
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
)
SELECT 
    airport AS airport_name,
    total_flights,
    delayed_flights,
    delay_percentage
FROM 
    airport_delays
ORDER BY 
    delayed_flights DESC
LIMIT 5;