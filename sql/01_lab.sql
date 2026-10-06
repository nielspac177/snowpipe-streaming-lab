-- =====================================================================
-- Snowpipe Streaming Lab · 01_lab.sql
-- Con productor_pagos.py corriendo en otra terminal.
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_STREAM;
USE SCHEMA STREAMING_LAB.PAGOS;

-- ---------------------------------------------------------------------
-- 1. ¿Está llegando? Corre varias veces y mira cómo crece.
-- ---------------------------------------------------------------------
SELECT COUNT(*)                                             AS filas,
       MAX(TS_EVENTO)                                       AS ultimo_evento,
       DATEDIFF('second', MAX(TS_EVENTO), CURRENT_TIMESTAMP()) AS frescura_seg
FROM PAGOS_STREAM;
-- Pregunta: ¿por qué frescura_seg no baja de ~5-10 s aunque el productor
-- mande 20 pagos por segundo? (buffer del cliente + commit del servidor)

-- El pipe por defecto ya existe aunque nadie lo creó:
SHOW PIPES IN SCHEMA STREAMING_LAB.PAGOS;

-- ---------------------------------------------------------------------
-- 2. Lo del deck: total y conteo de los últimos 5 minutos
-- ---------------------------------------------------------------------
SELECT COUNT(*) AS n, SUM(MONTO) AS total
FROM PAGOS_STREAM
WHERE TS_EVENTO >= DATEADD(minute, -5, CURRENT_TIMESTAMP());

-- Serie por minuto (para ver el flujo)
SELECT DATE_TRUNC('minute', TS_EVENTO) AS minuto, COUNT(*) AS n, SUM(MONTO) AS total
FROM PAGOS_STREAM
GROUP BY 1 ORDER BY 1 DESC LIMIT 15;

-- ---------------------------------------------------------------------
-- 3. Umbral fijo (deck) vs umbral estadístico (para ingenieros)
-- ---------------------------------------------------------------------
SELECT * FROM PAGOS_STREAM
WHERE TS_EVENTO >= DATEADD(minute, -1, CURRENT_TIMESTAMP())
  AND MONTO > 50000;

-- Umbral relativo: pagos > media + 4·desv. de su sucursal en la última hora.
-- Discusión: ¿qué pasa con una sucursal nueva sin historia? ¿y si el fraude
-- infla la propia desviación estándar? (por eso a veces se usa mediana/MAD)
WITH base AS (
  SELECT SUCURSAL, AVG(MONTO) AS media, STDDEV(MONTO) AS sd
  FROM PAGOS_STREAM
  WHERE TS_EVENTO >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
  GROUP BY SUCURSAL
)
SELECT p.*, ROUND((p.MONTO - b.media) / NULLIF(b.sd, 0), 1) AS z
FROM PAGOS_STREAM p JOIN base b USING (SUCURSAL)
WHERE p.TS_EVENTO >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
  AND p.MONTO > b.media + 4 * b.sd
ORDER BY p.TS_EVENTO DESC;

-- ---------------------------------------------------------------------
-- 4. Reaccionar sin que nadie corra la query: Stream + Task disparada
--    El stream guarda "qué filas son nuevas desde la última vez que lo leí".
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE ALERTAS_FRAUDE (
  PAGO_ID NUMBER, TS_EVENTO TIMESTAMP_TZ, SUCURSAL STRING, MONTO NUMBER(12,2),
  DETECTADO_EN TIMESTAMP_LTZ
);

CREATE OR REPLACE STREAM PAGOS_NUEVOS ON TABLE PAGOS_STREAM APPEND_ONLY = TRUE;

-- Sin SCHEDULE + WHEN SYSTEM$STREAM_HAS_DATA = task disparada por datos nuevos.
CREATE OR REPLACE TASK DETECTA_FRAUDE
  WAREHOUSE = WH_STREAM
  WHEN SYSTEM$STREAM_HAS_DATA('PAGOS_NUEVOS')
AS
  INSERT INTO ALERTAS_FRAUDE
  SELECT PAGO_ID, TS_EVENTO, SUCURSAL, MONTO, CURRENT_TIMESTAMP()
  FROM PAGOS_NUEVOS
  WHERE MONTO > 50000;

ALTER TASK DETECTA_FRAUDE RESUME;

-- Latencia de punta a punta: evento -> alerta registrada
SELECT *, DATEDIFF('second', TS_EVENTO, DETECTADO_EN) AS seg_evento_a_alerta
FROM ALERTAS_FRAUDE ORDER BY DETECTADO_EN DESC;

SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'DETECTA_FRAUDE'))
ORDER BY SCHEDULED_TIME DESC LIMIT 10;

-- ---------------------------------------------------------------------
-- 5. Agregado siempre fresco, declarativo: Dynamic Table
--    Tú declaras el resultado y qué tan viejo puede estar (TARGET_LAG);
--    Snowflake decide cuándo y cómo refrescar (incremental si puede).
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE VENTAS_POR_MINUTO
  TARGET_LAG = '1 minute'
  WAREHOUSE = WH_STREAM
AS
  SELECT DATE_TRUNC('minute', TS_EVENTO) AS minuto, SUCURSAL, CANAL,
         COUNT(*) AS n, SUM(MONTO) AS total
  FROM PAGOS_STREAM
  GROUP BY 1, 2, 3;

SELECT * FROM VENTAS_POR_MINUTO ORDER BY minuto DESC LIMIT 20;

SELECT NAME, REFRESH_ACTION, STATE, REFRESH_START_TIME, REFRESH_END_TIME
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME => 'VENTAS_POR_MINUTO'))
ORDER BY REFRESH_START_TIME DESC LIMIT 10;

-- ---------------------------------------------------------------------
-- 6. ¿Cuánto cuesta? (la ingesta se cobra por GB sin comprimir;
--    el stream/task/dynamic table consumen warehouse aparte)
--    Nota: ACCOUNT_USAGE tiene retraso de hasta ~horas; puede salir vacío en clase.
-- ---------------------------------------------------------------------
SELECT SERVICE_TYPE, NAME, SUM(CREDITS_USED) AS creditos
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2 ORDER BY creditos DESC;

-- ---------------------------------------------------------------------
-- 7. LIMPIEZA (importante en cuentas trial: la task y la DT gastan créditos)
-- ---------------------------------------------------------------------
ALTER TASK DETECTA_FRAUDE SUSPEND;
ALTER DYNAMIC TABLE VENTAS_POR_MINUTO SUSPEND;
-- DROP DATABASE STREAMING_LAB;
