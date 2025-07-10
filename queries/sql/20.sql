-- Query 20: Cross-airline performance benchmarking and ranking analysis
-- Performs comprehensive airline performance comparison across multiple KPIs
-- demonstrating advanced window functions, percentile analysis, and performance scoring

WITH airline_performance_metrics AS (
    SELECT 
        a.airline,
        COUNT(*) AS total_flights,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.arr_delay), 2) AS avg_arrival_delay,
        ROUND(AVG(f.air_time), 2) AS avg_air_time,
        ROUND(AVG(f.distance), 2) AS avg_distance,
        ROUND(AVG(f.actual_elapsed_time), 2) AS avg_elapsed_time,
        SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) AS cancelled_flights,
        SUM(CASE WHEN f.diverted > 0 THEN 1 ELSE 0 END) AS diverted_flights,
        SUM(CASE WHEN f.dep_delay > 15 THEN 1 ELSE 0 END) AS significantly_delayed_departures,
        SUM(CASE WHEN f.arr_delay > 15 THEN 1 ELSE 0 END) AS significantly_delayed_arrivals,
        SUM(f.distance) AS total_distance_miles,
        COUNT(DISTINCT f.origin) AS unique_origin_airports,
        COUNT(DISTINCT f.dest) AS unique_destination_airports,
        COUNT(DISTINCT CONCAT(f.origin, '-', f.dest)) AS unique_routes
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
        a.airline
    HAVING 
        COUNT(*) >= 10000
),
performance_rates AS (
    SELECT 
        apm.*,
        ROUND(apm.cancelled_flights * 100.0 / apm.total_flights, 2) AS cancellation_rate_pct,
        ROUND(apm.diverted_flights * 100.0 / apm.total_flights, 2) AS diversion_rate_pct,
        ROUND(apm.significantly_delayed_departures * 100.0 / apm.total_flights, 2) AS departure_delay_rate_pct,
        ROUND(apm.significantly_delayed_arrivals * 100.0 / apm.total_flights, 2) AS arrival_delay_rate_pct,
        ROUND(apm.avg_air_time / NULLIF(apm.avg_distance, 0) * 60, 2) AS operational_speed_mph,
        ROUND(apm.total_distance_miles / 1000000.0, 2) AS total_distance_millions,
        ROUND(apm.total_flights / apm.unique_routes, 2) AS avg_frequency_per_route
    FROM 
        airline_performance_metrics apm
),
percentile_rankings AS (
    SELECT 
        pr.*,
        NTILE(4) OVER (ORDER BY pr.avg_departure_delay ASC) AS departure_delay_quartile,
        NTILE(4) OVER (ORDER BY pr.avg_arrival_delay ASC) AS arrival_delay_quartile,
        NTILE(4) OVER (ORDER BY pr.cancellation_rate_pct ASC) AS reliability_quartile,
        NTILE(4) OVER (ORDER BY pr.operational_speed_mph DESC) AS efficiency_quartile,
        RANK() OVER (ORDER BY pr.avg_departure_delay ASC) AS departure_delay_rank,
        RANK() OVER (ORDER BY pr.avg_arrival_delay ASC) AS arrival_delay_rank,
        RANK() OVER (ORDER BY pr.cancellation_rate_pct ASC) AS reliability_rank,
        RANK() OVER (ORDER BY pr.total_flights DESC) AS market_size_rank,
        RANK() OVER (ORDER BY pr.unique_routes DESC) AS network_coverage_rank
    FROM 
        performance_rates pr
),
performance_scoring AS (
    SELECT 
        pr_rank.*,
        ROUND((
            (5 - pr_rank.departure_delay_quartile) * 0.25 +
            (5 - pr_rank.arrival_delay_quartile) * 0.25 +
            (5 - pr_rank.reliability_quartile) * 0.25 +
            pr_rank.efficiency_quartile * 0.25
        ), 2) AS composite_performance_score,
        CASE 
            WHEN pr_rank.departure_delay_quartile = 1 AND pr_rank.arrival_delay_quartile = 1 AND pr_rank.reliability_quartile = 1 THEN 'Excellent'
            WHEN pr_rank.departure_delay_quartile <= 2 AND pr_rank.arrival_delay_quartile <= 2 AND pr_rank.reliability_quartile <= 2 THEN 'Good'
            WHEN pr_rank.departure_delay_quartile >= 3 OR pr_rank.arrival_delay_quartile >= 3 OR pr_rank.reliability_quartile >= 3 THEN 'Needs Improvement'
            ELSE 'Average'
        END AS performance_category
    FROM 
        percentile_rankings pr_rank
)
SELECT 
    airline,
    total_flights,
    total_distance_millions,
    unique_routes,
    network_coverage_rank,
    avg_departure_delay,
    departure_delay_rank,
    avg_arrival_delay,
    arrival_delay_rank,
    cancellation_rate_pct,
    reliability_rank,
    diversion_rate_pct,
    departure_delay_rate_pct,
    arrival_delay_rate_pct,
    operational_speed_mph,
    avg_frequency_per_route,
    composite_performance_score,
    performance_category,
    departure_delay_quartile,
    arrival_delay_quartile,
    reliability_quartile,
    efficiency_quartile
FROM 
    performance_scoring
ORDER BY 
    composite_performance_score DESC, total_flights DESC;
