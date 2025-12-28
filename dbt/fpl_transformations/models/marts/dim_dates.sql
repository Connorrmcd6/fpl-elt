{{ config(materialized='table', database='marts') }}

-- Date dimension for time-based analysis
WITH date_spine AS (
    SELECT 
        toDate('2024-08-01') + number as date_day
    FROM numbers(365)
)

SELECT
    date_day,
    toDayOfWeek(date_day) as day_of_week,
    toDayOfMonth(date_day) as day_of_month,
    toDayOfYear(date_day) as day_of_year,
    toWeek(date_day) as week_of_year,
    toMonth(date_day) as month,
    toYear(date_day) as year,
    toStartOfWeek(date_day) as week_start,
    toStartOfMonth(date_day) as month_start,
    
    -- FPL specific
    CASE 
        WHEN toDayOfWeek(date_day) IN (6, 7) THEN true 
        ELSE false 
    END as is_weekend,
    
    -- Quarter
    ceil(toMonth(date_day) / 3.0) as quarter

FROM date_spine
WHERE date_day <= today()