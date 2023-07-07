/* ==========================

Testing DATEADD Funtion from SQL Scripts as YEAR was throwing errors.

FAILS on year, within the DATEADD Function. Probably a dateadd function parsing issue.
i.e. DATEADD(year, 1, '2023/07/05') FAILS

========================== */

SELECT  DATEADD(year, 1, '2017/08/25') AS DateAdd
FROM unienrollements t1