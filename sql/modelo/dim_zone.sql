CREATE OR REPLACE TABLE dim_zone AS
SELECT LocationID AS location_id,
       Borough    AS borough,
       Zone       AS zone,
       service_zone
FROM read_csv('{ZONAS}');