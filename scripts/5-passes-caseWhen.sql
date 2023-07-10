SELECT 
    CASE 
        WHEN t1.version_date IS NULL THEN NULL 
        ELSE CASE 
            WHEN t1.id_currency_months < t1.fonps_expiry THEN 'Current' 
            ELSE 'Expired'
        END 
    END AS id_currency,
    amount
FROM currencyTable t1