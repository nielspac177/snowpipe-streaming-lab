-- =====================================================================
-- Snowpipe Streaming Lab · 00_setup.sql
-- Cada alumno en su propia cuenta (trial sirve). Correr como ACCOUNTADMIN.
-- ACCOUNTADMIN solo porque es una cuenta de práctica desechable. En producción
-- el productor usa un rol propio con privilegios mínimos sobre la tabla y el pipe.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS WH_STREAM
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE IF NOT EXISTS STREAMING_LAB;
CREATE SCHEMA  IF NOT EXISTS STREAMING_LAB.PAGOS;
USE SCHEMA STREAMING_LAB.PAGOS;

-- Tabla destino. Las columnas del productor se mapean por nombre
-- (el pipe por defecto, PAGOS_STREAM-STREAMING, se crea solo al abrir el canal).
CREATE OR REPLACE TABLE PAGOS_STREAM (
  PAGO_ID     NUMBER,
  TS_EVENTO   TIMESTAMP_TZ,
  CLIENTE_ID  NUMBER,
  SUCURSAL    STRING,
  CANAL       STRING,
  MONTO       NUMBER(12,2)
);

-- Autenticación por llave (el SDK no acepta usuario/contraseña).
-- En terminal, antes:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- Pega el contenido de rsa_key.pub SIN las líneas BEGIN/END ni saltos de línea:
ALTER USER IDENTIFIER(CURRENT_USER()) SET RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';

-- Datos para profile.json:
SELECT CURRENT_USER() AS user,
       CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account,
       'https://' || CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME()
         || '.snowflakecomputing.com' AS url;
