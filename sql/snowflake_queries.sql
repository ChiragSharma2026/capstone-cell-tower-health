-- Q1 (from brief, worked)
WITH th AS (
  SELECT tower_id, hour_of_day, SUM(calls) AS calls, SUM(dropped_calls) AS drops
  FROM GOLD_TOWER_HOUR GROUP BY tower_id, hour_of_day)
SELECT tower_id, hour_of_day, calls, drops,
  ROUND(100.0*drops/NULLIF(calls,0),2) AS worst_hour_pct,
  ROUND(100.0*SUM(drops) OVER (PARTITION BY tower_id)
      / NULLIF(SUM(calls) OVER (PARTITION BY tower_id),0),2) AS tower_pct
FROM th
QUALIFY ROW_NUMBER() OVER (PARTITION BY tower_id
  ORDER BY drops/NULLIF(calls,0) DESC, hour_of_day) = 1
ORDER BY worst_hour_pct DESC;

-- Q2: towers >5% drop rate on more than 10 of 15 days
WITH daily AS (
  SELECT tower_id, call_date, SUM(calls) calls, SUM(dropped_calls) drops
  FROM GOLD_TOWER_HOUR GROUP BY tower_id, call_date),
flagged AS (
  SELECT tower_id,
    CASE WHEN 100.0*drops/NULLIF(calls,0) > 5 THEN 1 ELSE 0 END AS over5
  FROM daily)
SELECT tower_id, SUM(over5) AS days_over_5pct
FROM flagged GROUP BY tower_id
HAVING SUM(over5) > 10
ORDER BY days_over_5pct DESC;

-- Q3: drop rate vs utilisation
SELECT ROUND(utilisation,1) AS util_bucket,
  SUM(dropped_calls) drops, SUM(calls) calls,
  ROUND(100.0*SUM(dropped_calls)/NULLIF(SUM(calls),0),2) AS drop_pct
FROM GOLD_TOWER_HOUR
GROUP BY ROUND(utilisation,1)
ORDER BY util_bucket;