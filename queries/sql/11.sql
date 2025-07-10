-- Query 11: Top routes analysis by flight volume and performance
-- Analyzes the busiest routes (origin-destination pairs) by flight volume 
-- and calculates average flight time and distance, demonstrating star schema joins

WITH route_stats AS (
    SELECT 
        f.origin,
        f.dest,
        a.airline,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.air_time), 2) AS avg_flight_time_minutes,
        ROUND(AVG(f.distance), 2) AS avg_distance_miles,
        ROUND(AVG(f.actual_elapsed_time), 2) AS avg_elapsed_time_minutes,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.year = 2020
        AND f.air_time IS NOT NULL
        AND f.distance IS NOT NULL
        AND f.actual_elapsed_time IS NOT NULL
    GROUP BY 
        f.origin, f.dest, a.airline
),
route_rankings AS (
    SELECT 
        rs.*,
        ROW_NUMBER() OVER (PARTITION BY rs.origin, rs.dest ORDER BY rs.total_flights DESC) as airline_rank_on_route
    FROM 
        route_stats rs
    WHERE 
        rs.total_flights >= 50
)
SELECT 
    ap_orig.airport AS origin_airport,
    ap_orig.city AS origin_city,
    ap_orig.state AS origin_state,
    ap_dest.airport AS destination_airport,
    ap_dest.city AS destination_city,
    ap_dest.state AS destination_state,
    rr.airline,
    rr.total_flights,
    rr.avg_flight_time_minutes,
    rr.avg_distance_miles,
    rr.avg_elapsed_time_minutes,
    rr.avg_departure_delay,
    rr.avg_arrival_delay,
    rr.airline_rank_on_route
FROM 
    route_rankings rr
JOIN 
    airports ap_orig ON rr.origin = ap_orig.iata_code
JOIN 
    airports ap_dest ON rr.dest = ap_dest.iata_code
WHERE 
    rr.airline_rank_on_route <= 2
ORDER BY 
    rr.total_flights DESC
LIMIT 30;
