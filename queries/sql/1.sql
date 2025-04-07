-- This query calculates airline performance statistics for the year 2020.
-- It computes the total number of flights, market share percentage, 
-- percentage of cancelled flights, and percentage of diverted flights for each airline.

WITH flight_stats AS (
    SELECT 
        a.airline,
        COUNT(*) AS volume,
        SUM(diverted) AS diverted,
        SUM(cancelled) AS cancelled
    FROM 
        flights f
    JOIN 
        airlines a 
    ON 
        f.carrier = a.iata_code
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
