CREATE OR REPLACE TABLE GOLD_TOWER_HOUR (
  tower_id STRING, site_name STRING, city STRING, district STRING,
  capacity_channels INT, call_date DATE, hour_of_day INT,
  calls INT, dropped_calls INT, drop_rate FLOAT,
  peak_concurrent INT, utilisation FLOAT, avg_duration_seconds FLOAT
);

CREATE OR REPLACE STAGE capstone_stage;
-- upload the CSV into this stage via Snowsight UI, then:

COPY INTO GOLD_TOWER_HOUR
FROM @capstone_stage/tower_hour_export.csv
FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- run COPY INTO again to prove idempotency — must report 0 rows loaded
COPY INTO GOLD_TOWER_HOUR
FROM @capstone_stage/tower_hour_export.csv
FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');