-- Query 4: Daily arrival delays for Bay Area airports in November 2020
-- Analyzes average and maximum arrival delays for flights arriving at SFO, OAK, SJC
-- in November 2020, grouped by destination and day, demonstrating fact table analysis

WITH november_delays AS (
    SELECT 
        f.dest,
        f.month,
        CASE 
            WHEN f.month = 1 THEN 'January'
            WHEN f.month = 2 THEN 'February'
            WHEN f.month = 3 THEN 'March'
            WHEN f.month = 4 THEN 'April'
            WHEN f.month = 5 THEN 'May'
            WHEN f.month = 6 THEN 'June'
            WHEN f.month = 7 THEN 'July'
            WHEN f.month = 8 THEN 'August'
            WHEN f.month = 9 THEN 'September'
            WHEN f.month = 10 THEN 'October'
            WHEN f.month = 11 THEN 'November'
            WHEN f.month = 12 THEN 'December'
        END AS month_name,
        f.day,
        AVG(f.arr_delay) AS avg_arr_delay,
        MAX(f.arr_delay) AS max_arr_delay
    FROM 
        flights f
    WHERE 
        f.dest IN ('SFO', 'OAK', 'SJC')
        AND f.arr_delay > 0
        AND f.month = 11
        AND f.year = 2020
    GROUP BY 
        f.dest, f.month, f.day
)
SELECT 
    dest,
    month,
    month_name,
    day,
    avg_arr_delay,
    max_arr_delay
FROM 
    november_delays
ORDER BY 
    dest, month, day;