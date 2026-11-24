SELECT
  CAST(LocationID AS INTEGER) AS location_id,
  Borough AS borough,
  Zone AS zone,
  service_zone
FROM read_csv('{{ var("zones_csv_path") }}')
