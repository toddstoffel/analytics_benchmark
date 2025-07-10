-- Query 7: Airline distance and air time statistics by year
-- Calculates total distance traveled and average air time for each airline
-- by year, demonstrating fact/dimension joins

WITH airline_distance_stats AS (
    SELECT 
        a.airline,
        f.year,
        SUM(f.distance) AS total_distance_traveled,
        ROUND(AVG(f.air_time), 2) AS avg_air_time
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.distance IS NOT NULL
        AND f.air_time IS NOT NULL
    GROUP BY 
        a.airline, f.year
)
SELECT 
    airline AS Airline,
    year AS Year,
    total_distance_traveled AS Total_Distance_Traveled,
    avg_air_time AS Avg_Air_Time
FROM 
    airline_distance_stats
ORDER BY 
    year, total_distance_traveled DESC;