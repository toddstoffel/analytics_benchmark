-- Query 6: Yearly airline performance statistics
-- Calculates total flights, average departure delay, and average arrival delay
-- for each airline by year, demonstrating fact/dimension joins

WITH airline_yearly_stats AS (
    SELECT 
        a.airline,
        f.year,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.fl_date IS NOT NULL
    GROUP BY 
        a.airline, f.year
)
SELECT 
    airline AS Airline,
    year AS Year,
    total_flights AS Total_Flights,
    avg_departure_delay AS Avg_Departure_Delay,
    avg_arrival_delay AS Avg_Arrival_Delay
FROM 
    airline_yearly_stats
ORDER BY 
    year, total_flights DESC;