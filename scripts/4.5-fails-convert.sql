SELECT  t1.ID,
        t1.NAME,
        CAST(t1.v3 AS VARCHAR(24)) + CHAR(10) + CONVERT(INT, t1.v2) AS versionString
FROM unienrollments t1