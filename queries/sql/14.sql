-- Query 14: Hub airport efficiency analysis
-- Analyzes major airports by comparing airline operations, market share, and efficiency
-- demonstrating complex star schema joins with advanced analytical functions

WITH airport_volume_analysis AS (
    SELECT 
        ap.airport,
        ap.city,
        ap.state,
        COUNT(*) AS total_departures,
        COUNT(DISTINCT f.carrier) AS airlines_serving,
        ROUND(AVG(f.dep_delay), 2) AS overall_avg_delay
    FROM 
        flights f
    JOIN 
        airports ap ON f.origin = ap.iata_code
    WHERE 
        f.year = 2020
    GROUP BY 
        ap.airport, ap.city, ap.state
    HAVING 
        COUNT(*) >= 1000
),
airline_hub_performance AS (
    SELECT 
        a.airline,
        ap.airport,
        ap.city,
        ap.state,
        COUNT(*) AS airline_departures,
        ROUND(AVG(f.dep_delay), 2) AS avg_departure_delay,
        ROUND(AVG(f.taxi_out), 2) AS avg_taxi_out_time,
        ROUND(AVG(f.crs_elapsed_time), 2) AS avg_scheduled_time,
        ROUND(AVG(f.actual_elapsed_time), 2) AS avg_actual_time,
        ROUND(SUM(CASE WHEN f.cancelled > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancellation_rate_pct,
        ROUND(SUM(CASE WHEN f.dep_delay > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS significant_delay_rate_pct
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    JOIN 
        airports ap ON f.origin = ap.iata_code
    WHERE 
        f.year = 2020
        AND f.taxi_out IS NOT NULL
        AND f.crs_elapsed_time IS NOT NULL
        AND ap.airport IN (SELECT airport FROM airport_volume_analysis)
    GROUP BY 
        a.airline, ap.airport, ap.city, ap.state
    HAVING 
        COUNT(*) >= 50
),
market_share_analysis AS (
    SELECT 
        ahp.*,
        ava.total_departures AS hub_total_departures,
        ROUND(ahp.airline_departures * 100.0 / ava.total_departures, 2) AS market_share_pct,
        RANK() OVER (PARTITION BY ahp.airport ORDER BY ahp.airline_departures DESC) AS market_position,
        RANK() OVER (PARTITION BY ahp.airport ORDER BY ahp.avg_departure_delay ASC) AS efficiency_rank
    FROM 
        airline_hub_performance ahp
    JOIN 
        airport_volume_analysis ava ON ahp.airport = ava.airport
),
hub_leaders AS (
    SELECT 
        msa.*,
        CASE 
            WHEN msa.market_position = 1 THEN 'Hub Leader'
            WHEN msa.market_position <= 3 THEN 'Major Player'
            WHEN msa.market_share_pct >= 10 THEN 'Significant Presence'
            ELSE 'Minor Player'
        END AS hub_status,
        CASE 
            WHEN msa.efficiency_rank = 1 THEN 'Most Efficient'
            WHEN msa.efficiency_rank <= 3 THEN 'High Efficiency'
            ELSE 'Standard Efficiency'
        END AS efficiency_status
    FROM 
        market_share_analysis msa
)
SELECT 
    hl.airport,
    hl.city,
    hl.state,
    hl.airline,
    hl.airline_departures,
    hl.hub_total_departures,
    hl.market_share_pct,
    hl.hub_status,
    hl.avg_departure_delay,
    hl.avg_taxi_out_time,
    hl.cancellation_rate_pct,
    hl.significant_delay_rate_pct,
    hl.efficiency_status,
    hl.market_position,
    hl.efficiency_rank
FROM 
    hub_leaders hl
WHERE 
    hl.market_share_pct >= 5.0
ORDER BY 
    hl.airport, hl.market_position;
