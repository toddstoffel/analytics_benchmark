-- Query 9: Average delays by airline and year
-- Calculates average arrival and departure delays for each airline by year
-- demonstrating fact/dimension joins and time-based analysis

WITH airline_delays AS (
    SELECT
        f.year,
        a.airline,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay
    FROM
        flights f
    JOIN
        airlines a ON f.carrier = a.iata_code
    GROUP BY
        f.year, a.airline
)
SELECT
    year,
    airline AS airline_name,
    avg_arrival_delay,
    avg_departure_delay
FROM
    airline_delays
ORDER BY
    year, avg_arrival_delay DESC;