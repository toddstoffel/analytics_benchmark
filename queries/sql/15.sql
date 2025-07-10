-- Query 15: Airline operational efficiency by distance segments and markets
-- Analyzes airline performance across flight distance segments and geographic markets
-- demonstrating complex star schema joins with advanced analytical segmentation

WITH distance_segments AS (
    SELECT 
        CASE 
            WHEN f.distance <= 500 THEN 'Short-haul (0-500 miles)'
            WHEN f.distance <= 1000 THEN 'Medium-haul (501-1000 miles)'
            WHEN f.distance <= 2000 THEN 'Long-haul (1001-2000 miles)'
            WHEN f.distance > 2000 THEN 'Ultra-long-haul (2000+ miles)'
        END AS distance_category,
        f.distance,
        f.carrier,
        f.origin,
        f.dest,
        f.air_time,
        f.actual_elapsed_time,
        f.dep_delay,
        f.arr_delay,
        f.taxi_out,
        f.taxi_in,
        f.cancelled,
        f.diverted
    FROM 
        flights f
    WHERE 
        f.year = 2020
        AND f.distance IS NOT NULL
        AND f.distance > 0
),
geographic_markets AS (
    SELECT 
        ds.*,
        a.airline,
        ap_orig.state AS origin_state,
        ap_dest.state AS dest_state,
        CASE 
            WHEN ap_orig.state = ap_dest.state THEN 'Domestic'
            ELSE 'Interstate'
        END AS market_type
    FROM 
        distance_segments ds
    JOIN 
        airlines a ON ds.carrier = a.iata_code
    JOIN 
        airports ap_orig ON ds.origin = ap_orig.iata_code
    JOIN 
        airports ap_dest ON ds.dest = ap_dest.iata_code
),
airline_segment_performance AS (
    SELECT 
        gm.airline,
        gm.distance_category,
        gm.market_type,
        COUNT(*) AS total_flights,
        ROUND(AVG(gm.distance), 2) AS avg_distance_miles,
        ROUND(AVG(gm.air_time), 2) AS avg_air_time_minutes,
        ROUND(AVG(gm.actual_elapsed_time), 2) AS avg_total_time_minutes,
        ROUND(AVG(gm.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(gm.arr_delay), 2) AS avg_arrival_delay,
        ROUND(AVG(gm.taxi_out), 2) AS avg_taxi_out_time,
        ROUND(AVG(gm.taxi_in), 2) AS avg_taxi_in_time,
        ROUND(SUM(CASE WHEN gm.cancelled > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_rate_pct,
        ROUND(SUM(CASE WHEN gm.diverted > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS diversion_rate_pct,
        ROUND(SUM(CASE WHEN gm.arr_delay > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS significant_delay_rate_pct
    FROM 
        geographic_markets gm
    GROUP BY 
        gm.airline, gm.distance_category, gm.market_type
    HAVING 
        COUNT(*) >= 30
),
efficiency_rankings AS (
    SELECT 
        asp.*,
        RANK() OVER (PARTITION BY asp.distance_category, asp.market_type ORDER BY asp.avg_arrival_delay ASC) AS delay_efficiency_rank,
        RANK() OVER (PARTITION BY asp.distance_category, asp.market_type ORDER BY asp.cancellation_rate_pct ASC) AS reliability_rank,
        RANK() OVER (PARTITION BY asp.distance_category, asp.market_type ORDER BY asp.total_flights DESC) AS market_share_rank,
        COUNT(*) OVER (PARTITION BY asp.distance_category, asp.market_type) AS total_competitors
    FROM 
        airline_segment_performance asp
),
performance_categories AS (
    SELECT 
        er.*,
        CASE 
            WHEN er.delay_efficiency_rank = 1 THEN 'Best in Class'
            WHEN er.delay_efficiency_rank <= 3 THEN 'Top Performer'
            WHEN er.delay_efficiency_rank > er.total_competitors - 2 THEN 'Needs Improvement'
            ELSE 'Average'
        END AS performance_tier,
        ROUND(er.avg_air_time_minutes / NULLIF(er.avg_distance_miles, 0) * 60, 2) AS time_efficiency_ratio
    FROM 
        efficiency_rankings er
)
SELECT 
    pc.airline,
    pc.distance_category,
    pc.market_type,
    pc.total_flights,
    pc.avg_distance_miles,
    pc.avg_air_time_minutes,
    pc.avg_departure_delay,
    pc.avg_arrival_delay,
    pc.cancellation_rate_pct,
    pc.significant_delay_rate_pct,
    pc.time_efficiency_ratio,
    pc.delay_efficiency_rank,
    pc.reliability_rank,
    pc.market_share_rank,
    pc.performance_tier
FROM 
    performance_categories pc
WHERE 
    pc.total_flights >= 50
ORDER BY 
    pc.distance_category, pc.market_type, pc.delay_efficiency_rank;
