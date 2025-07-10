-- Query 2: Airline delay analysis by type and year
-- Analyzes different types of delays (Airline Delay, Late Aircraft Delay, 
-- Air System Delay, and Weather Delay) for each airline by year, demonstrating fact/dimension joins

WITH airline_delays AS (
    SELECT 
        a.airline,
        f.year,
        'Airline Delay' AS delay_type,
        COUNT(*) AS delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.carrier_delay > 0
    GROUP BY 
        a.airline, f.year

    UNION ALL 

    SELECT 
        a.airline,
        f.year,
        'Late Aircraft Delay' AS delay_type,
        COUNT(*) AS delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.late_aircraft_delay > 0
    GROUP BY 
        a.airline, f.year

    UNION ALL 

    SELECT 
        a.airline,
        f.year,
        'Air System Delay' AS delay_type,
        COUNT(*) AS delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.nas_delay > 0
    GROUP BY 
        a.airline, f.year

    UNION ALL 

    SELECT 
        a.airline,
        f.year,
        'Weather Delay' AS delay_type,
        COUNT(*) AS delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.weather_delay > 0
    GROUP BY 
        a.airline, f.year
)
SELECT 
    ad.airline,
    ad.year,
    ad.delay_type,
    ad.delay
FROM 
    airline_delays ad
ORDER BY 
    ad.airline, ad.year, ad.delay_type;
