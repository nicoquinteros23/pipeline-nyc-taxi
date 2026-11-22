SELECT * FROM {{ ref('stg_yellow_trips_2024_03') }}
UNION ALL BY NAME
SELECT * FROM {{ ref('stg_yellow_trips_2025_03') }}
