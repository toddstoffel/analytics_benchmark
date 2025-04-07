-- This query calculates yearly airline performance statistics.
-- It computes the total number of flights, average departure delay, 
-- and average arrival delay for each airline, grouped by airline and year.

SELECT 
    a.airline AS Airline,
    f.year AS Year,
    COUNT(*) AS Total_Flights,
    ROUND(AVG(f.dep_delay), 2) AS Avg_Departure_Delay,
    ROUND(AVG(f.arr_delay), 2) AS Avg_Arrival_Delay
FROM 
    flights f
JOIN 
    airlines a ON f.carrier = a.iata_code
WHERE 
    f.fl_date IS NOT NULL
GROUP BY 
    a.airline, f.year
ORDER BY 
    Year, Total_Flights DESC;