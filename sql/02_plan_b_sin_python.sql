-- =====================================================================
-- Plan B: si la llave RSA o el pip install fallan, simulamos la llegada
-- con INSERTs. Las secciones 2-7 de 01_lab.sql funcionan igual.
-- (Ojo: esto es batch disfrazado; sirve para discutir la diferencia.)
-- =====================================================================
USE WAREHOUSE WH_STREAM;
USE SCHEMA STREAMING_LAB.PAGOS;

-- Historia: 3,000 pagos repartidos en los últimos 60 minutos
INSERT INTO PAGOS_STREAM
SELECT SEQ4() + 1,
       DATEADD(second, -UNIFORM(0, 3600, RANDOM()), CURRENT_TIMESTAMP()),
       UNIFORM(1, 5000, RANDOM()),
       ARRAY_CONSTRUCT('MTY-CENTRO','MTY-APODACA','SALTILLO','CHIHUAHUA','LIMA','GUAYAQUIL')[UNIFORM(0,5,RANDOM())]::STRING,
       ARRAY_CONSTRUCT('TRADICIONAL','MODERNO','B2B-APP')[UNIFORM(0,2,RANDOM())]::STRING,
       ROUND(EXP(NORMAL(5.5, 0.9, RANDOM())), 2)
FROM TABLE(GENERATOR(ROWCOUNT => 3000));

-- "Llegan" 200 pagos nuevos de los últimos 30 s (correr antes de cada refresh)
INSERT INTO PAGOS_STREAM
SELECT (SELECT MAX(PAGO_ID) FROM PAGOS_STREAM) + SEQ4() + 1,
       DATEADD(second, -UNIFORM(0, 30, RANDOM()), CURRENT_TIMESTAMP()),
       UNIFORM(1, 5000, RANDOM()),
       ARRAY_CONSTRUCT('MTY-CENTRO','MTY-APODACA','SALTILLO','CHIHUAHUA','LIMA','GUAYAQUIL')[UNIFORM(0,5,RANDOM())]::STRING,
       ARRAY_CONSTRUCT('TRADICIONAL','MODERNO','B2B-APP')[UNIFORM(0,2,RANDOM())]::STRING,
       ROUND(EXP(NORMAL(5.5, 0.9, RANDOM())), 2)
FROM TABLE(GENERATOR(ROWCOUNT => 200));

-- El pago fraudulento
INSERT INTO PAGOS_STREAM
SELECT MAX(PAGO_ID) + 1, CURRENT_TIMESTAMP(), 4242, 'SALTILLO', 'B2B-APP', 78000
FROM PAGOS_STREAM;
