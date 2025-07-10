-- Query 8: Airline market share and cancellation analysis for 2020
-- Calculates total flights, market share percentage, and cancellation percentage
-- for each airline, demonstrating fact/dimension joins and window functions

WITH airline_stats AS (
    SELECT 
        a.airline,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) AS cancelled_flights,
        SUM(COUNT(*)) OVER () AS total_market_flights
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.year = 2020
    GROUP BY 
        a.airline
)
SELECT 
    airline AS airline_name,
    total_flights,
    ROUND(total_flights * 100.0 / total_market_flights, 2) AS market_share_percentage,
    ROUND(cancelled_flights * 100.0 / total_flights, 2) AS cancellation_percentage
FROM 
    airline_stats
ORDER BY 
    market_share_percentage DESC;