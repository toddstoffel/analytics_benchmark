-- This query generates a report of total distance traveled and average air time 
-- for each airline, grouped by airline and year. It calculates the total distance 
-- traveled and the average air time for flights where both values are available.

SELECT 
    a.airline AS Airline,
    f.year AS Year,
    SUM(f.distance) AS Total_Distance_Traveled,
    ROUND(AVG(f.air_time), 2) AS Avg_Air_Time
FROM 
    flights f
JOIN 
    airlines a ON f.carrier = a.iata_code
WHERE 
    f.distance IS NOT NULL
    AND f.air_time IS NOT NULL
GROUP BY 
    a.airline, f.year
ORDER BY 
    Year, Total_Distance_Traveled DESC;