SELECT  t1.ID,
        t1.NAME,
        CAST(t1.v3 AS VARCHAR(24)) + CHAR(10) + CAST(t1.vd4 AS VARCHAR(24)) AS versionString
FROM unienrollments t1