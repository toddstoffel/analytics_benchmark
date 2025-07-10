-- Query 18: Competitive route analysis and market dynamics
-- Analyzes route competition by comparing airlines on the same routes
-- demonstrating complex star schema joins with competitive intelligence metrics

WITH route_competition AS (
    SELECT 
        f.origin,
        f.dest,
        a.airline,
        COUNT(*) AS flights_on_route,
        ROUND(AVG(f.distance), 2) AS avg_distance,
        ROUND(AVG(f.air_time), 2) AS avg_flight_time,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) AS cancelled_flights,
        COUNT(DISTINCT f.tail_num) AS unique_aircraft_count
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    WHERE 
        f.year = 2020
        AND f.air_time IS NOT NULL
        AND f.distance IS NOT NULL
    GROUP BY 
        f.origin, f.dest, a.airline
    HAVING 
        COUNT(*) >= 50
),
route_market_share AS (
    SELECT 
        rc.*,
        SUM(rc.flights_on_route) OVER (PARTITION BY rc.origin, rc.dest) AS total_route_flights,
        COUNT(*) OVER (PARTITION BY rc.origin, rc.dest) AS competitors_on_route,
        ROUND(rc.flights_on_route * 100.0 / SUM(rc.flights_on_route) OVER (PARTITION BY rc.origin, rc.dest), 2) AS market_share_pct,
        RANK() OVER (PARTITION BY rc.origin, rc.dest ORDER BY rc.flights_on_route DESC) AS market_position,
        ROUND(rc.cancelled_flights * 100.0 / rc.flights_on_route, 2) AS cancellation_rate_pct
    FROM 
        route_competition rc
),
competitive_routes AS (
    SELECT 
        rms.*,
        ap_orig.airport AS origin_airport,
        ap_orig.city AS origin_city,
        ap_orig.state AS origin_state,
        ap_dest.airport AS destination_airport,
        ap_dest.city AS destination_city,
        ap_dest.state AS destination_state,
        CASE 
            WHEN rms.competitors_on_route = 1 THEN 'Monopoly'
            WHEN rms.competitors_on_route = 2 THEN 'Duopoly'
            WHEN rms.competitors_on_route BETWEEN 3 AND 5 THEN 'Competitive'
            ELSE 'Highly Competitive'
        END AS competition_level,
        CASE 
            WHEN rms.market_share_pct >= 60 THEN 'Dominant'
            WHEN rms.market_share_pct >= 40 THEN 'Strong'
            WHEN rms.market_share_pct >= 20 THEN 'Moderate'
            ELSE 'Weak'
        END AS market_strength
    FROM 
        route_market_share rms
    JOIN 
        airports ap_orig ON rms.origin = ap_orig.iata_code
    JOIN 
        airports ap_dest ON rms.dest = ap_dest.iata_code
    WHERE 
        rms.competitors_on_route >= 2
        AND rms.total_route_flights >= 200
)
SELECT 
    origin_airport,
    origin_city,
    origin_state,
    destination_airport,
    destination_city,
    destination_state,
    airline,
    flights_on_route,
    market_share_pct,
    market_position,
    competition_level,
    market_strength,
    competitors_on_route,
    avg_departure_delay,
    avg_arrival_delay,
    cancellation_rate_pct,
    unique_aircraft_count
FROM 
    competitive_routes
ORDER BY 
    total_route_flights DESC, origin_airport, destination_airport, market_position;
