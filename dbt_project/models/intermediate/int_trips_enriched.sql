-- Las columnas derivadas (duración, velocidad, fecha/hora/día, % de propina)
-- y los nombres de zona ya vienen calculados desde la capa curada (src/calidad.py).
-- Este modelo solo expone esa capa a los marts.
SELECT *
FROM {{ source('pipeline_intermediate', 'trips_validated') }}
