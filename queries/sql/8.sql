-- This query calculates the total number of flights, market share percentage, 
-- and cancellation percentage for each airline in 2020.
SELECT 
    a.airline AS airline_name,
    COUNT(*) AS total_flights,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE year = 2020), 2) AS market_share_percentage,
    ROUND(SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_percentage
FROM 
    flights f
JOIN 
    airlines a ON f.carrier = a.iata_code
WHERE 
    f.year = 2020
GROUP BY 
    a.airline
ORDER BY 
    market_share_percentage DESC;