-- This query generates a report of different types of delays (Airline Delay, Late Aircraft Delay, 
-- Air System Delay, and Weather Delay) for each airline, grouped by airline and year. 
-- It calculates the total count of each delay type.

SELECT 
    q.airline,
    q.year,
    q.delay_type,
    q.delay
FROM (
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
) AS q
ORDER BY 
    q.airline, q.year, q.delay_type;
