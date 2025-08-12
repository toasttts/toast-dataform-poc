-- Mock raw data loader script

CREATE OR REPLACE TABLE raw.users AS (
  SELECT
    CAST(1 AS INT64) AS user_id,
    "alice@example.com" AS email,
    "2023-01-01" AS created_at,
    "premium" AS plan
);

CREATE OR REPLACE TABLE raw.sessions AS (
  SELECT
    CAST(101 AS INT64) AS session_id,
    CAST(1 AS INT64) AS user_id,
    TIMESTAMP("2023-07-01 10:00:00 UTC") AS session_start,
    "mobile" AS device_type
);

CREATE OR REPLACE TABLE raw.feature_events AS (
  SELECT
    CAST(1001 AS INT64) AS event_id,
    CAST(1 AS INT64) AS user_id,
    "feature_x_used" AS event_name,
    TIMESTAMP("2023-07-01 10:05:00 UTC") AS event_timestamp
);
