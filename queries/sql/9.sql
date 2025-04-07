-- This query calculates the average arrival delay and departure delay 
-- for each airline, grouped by year.
SELECT
    f.year,
    a.airline AS airline_name,
    ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
    ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay
FROM
    flights f
JOIN
    airlines a ON f.carrier = a.iata_code
GROUP BY
    f.year, a.airline
ORDER BY
    f.year, avg_arrival_delay DESC;