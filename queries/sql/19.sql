-- Query 19: Weather impact and seasonal disruption analysis
-- Analyzes the relationship between weather delays and operational disruptions
-- demonstrating conditional aggregations and seasonal pattern analysis

WITH weather_impact AS (
    SELECT 
        f.month,
        CASE 
            WHEN f.month IN (12, 1, 2) THEN 'Winter'
            WHEN f.month IN (3, 4, 5) THEN 'Spring'
            WHEN f.month IN (6, 7, 8) THEN 'Summer'
            WHEN f.month IN (9, 10, 11) THEN 'Fall'
        END AS season,
        ap.airport,
        ap.city,
        ap.state,
        a.airline,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN f.weather_delay > 0 THEN 1 ELSE 0 END) AS weather_delayed_flights,
        SUM(CASE WHEN f.cancelled = 1 AND f.cancellation_code = 'B' THEN 1 ELSE 0 END) AS weather_cancellations,
        SUM(CASE WHEN f.diverted > 0 THEN 1 ELSE 0 END) AS diverted_flights,
        ROUND(AVG(CASE WHEN f.weather_delay > 0 THEN f.weather_delay END), 2) AS avg_weather_delay_minutes,
        ROUND(AVG(CASE WHEN f.weather_delay > 0 THEN f.arr_delay END), 2) AS avg_total_delay_weather_affected,
        SUM(f.weather_delay) AS total_weather_delay_minutes,
        MAX(f.weather_delay) AS max_weather_delay_minutes
    FROM 
        flights f
    JOIN 
        airlines a ON f.carrier = a.iata_code
    JOIN 
        airports ap ON f.origin = ap.iata_code
    WHERE 
        f.year = 2020
    GROUP BY 
        f.month, ap.airport, ap.city, ap.state, a.airline
    HAVING 
        COUNT(*) >= 100
),
seasonal_rankings AS (
    SELECT 
        wi.*,
        ROUND(wi.weather_delayed_flights * 100.0 / wi.total_flights, 2) AS weather_delay_rate_pct,
        ROUND(wi.weather_cancellations * 100.0 / wi.total_flights, 2) AS weather_cancellation_rate_pct,
        ROUND(wi.diverted_flights * 100.0 / wi.total_flights, 2) AS diversion_rate_pct,
        ROUND(wi.total_weather_delay_minutes / 60.0, 2) AS total_weather_delay_hours
    FROM 
        weather_impact wi
    WHERE 
        wi.weather_delayed_flights > 0
),
weather_rankings AS (
    SELECT 
        sr.*,
        RANK() OVER (PARTITION BY sr.season ORDER BY sr.weather_delay_rate_pct DESC) AS worst_weather_airports,
        RANK() OVER (PARTITION BY sr.airport ORDER BY sr.weather_delay_rate_pct DESC) AS worst_weather_months,
        CASE 
            WHEN sr.weather_delay_rate_pct >= 10 THEN 'High Weather Impact'
            WHEN sr.weather_delay_rate_pct >= 5 THEN 'Moderate Weather Impact'
            WHEN sr.weather_delay_rate_pct >= 1 THEN 'Low Weather Impact'
            ELSE 'Minimal Weather Impact'
        END AS weather_impact_level
    FROM 
        seasonal_rankings sr
),
disruption_analysis AS (
    SELECT 
        wr.*,
        ROUND(wr.avg_weather_delay_minutes / NULLIF(wr.avg_total_delay_weather_affected, 0) * 100, 2) AS weather_delay_contribution_pct,
        SUM(wr.weather_delayed_flights) OVER (PARTITION BY wr.season) AS season_total_weather_delays,
        SUM(wr.total_flights) OVER (PARTITION BY wr.season) AS season_total_flights
    FROM 
        weather_rankings wr
)
SELECT 
    season,
    month,
    airport,
    city,
    state,
    airline,
    total_flights,
    weather_delayed_flights,
    weather_delay_rate_pct,
    weather_cancellations,
    weather_cancellation_rate_pct,
    diversion_rate_pct,
    avg_weather_delay_minutes,
    max_weather_delay_minutes,
    total_weather_delay_hours,
    weather_delay_contribution_pct,
    weather_impact_level,
    worst_weather_airports,
    worst_weather_months
FROM 
    disruption_analysis
WHERE 
    weather_delay_rate_pct >= 2.0
ORDER BY 
    season, weather_delay_rate_pct DESC, total_flights DESC;
