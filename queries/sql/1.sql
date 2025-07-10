-- Query 1: Airline performance statistics for 2020
-- Calculates total flights, market share percentage, cancellation percentage, 
-- and diversion percentage for each airline, demonstrating fact/dimension joins

WITH flight_stats AS (
    SELECT 
        a.airline,
        COUNT(*) AS volume,
        SUM(f.diverted) AS diverted,
        SUM(f.cancelled) AS cancelled
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.year = 2020
    GROUP BY 
        a.airline
),
total_volume AS (
    SELECT 
        SUM(volume) AS total_volume
    FROM 
        flight_stats
)
SELECT 
    fs.airline,
    fs.volume AS flight_count,
    ROUND(100 * fs.volume / tv.total_volume, 2) AS market_share_pct,
    ROUND(100 * (fs.cancelled / fs.volume), 2) AS cancelled_pct,
    ROUND(100 * (fs.diverted / fs.volume), 2) AS diverted_pct
FROM 
    flight_stats fs
CROSS JOIN 
    total_volume tv
ORDER BY 
    flight_count DESC;
