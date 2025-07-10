-- Query 5: California airport flight volumes and delays by airline
-- Analyzes flight volumes and average arrival delays for flights arriving at California airports
-- in 2020, demonstrating star schema joins between flights, airlines, and airports

WITH california_flights AS (
    SELECT 
        a.airline,
        ap.airport,
        COUNT(*) AS volume,
        AVG(f.arr_delay) AS avg_arrival_delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    JOIN 
        airports ap ON f.dest = ap.iata_code
    WHERE 
        ap.state = 'CA'
        AND f.year = 2020
    GROUP BY 
        a.airline, ap.airport
)
SELECT 
    airline,
    airport,
    volume,
    avg_arrival_delay
FROM 
    california_flights
ORDER BY 
    airline, airport;
