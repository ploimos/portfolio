-- 0. Conteggio totale transazioni e frodi
SELECT 
    COUNT(*) AS "Transazioni totali",
    (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud) AS "Numero di frodi",
    ROUND(100.0 * (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud) / COUNT(*), 2) AS "Percentuale di frodi",
    MIN(amount) AS "Importo minimo (frode)",
    MAX(amount) AS "Importo massimo (frode)",
    ROUND(AVG(amount), 2) AS "Importo medio (frode)"
FROM fact_transactions;

-- 1. Frodi per ora del giorno
SELECT 
    DATE_PART('hour', transaction_date + gmt_offset * INTERVAL '1 hour') AS "Ora Locale",
    COUNT(*) AS "Transazioni totali",
    SUM(is_fraud::int) AS "Numero di frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale di frodi",
    ROUND(AVG(amount), 2) AS "Importo medio (frode)"
FROM fact_transactions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY "Ora Locale"
ORDER BY "Percentuale di frodi" DESC;

-- 2. Distribuzione Geografica delle Frodi
SELECT 
    c.country_name as "Nome Paese",
    c.zone as "Zona",
    COUNT(*) AS "Transazioni totali",
    SUM(is_fraud::int) AS "Numero di frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale di frodi",
    ROUND(AVG(amount), 2) AS "Transazione media in frodi"
FROM fact_transactions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY "Nome Paese", "Zona"
ORDER BY "Transazione media in frodi" DESC
LIMIT 10;

-- 3. Analisi per Tipo di Carta
SELECT
    ct.type_name AS "Tipo carta",
    cb.brand_name AS "Marchio carta",
    COUNT(*) AS "Numero transazioni",
    SUM(is_fraud::int) AS "Numero frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions f
JOIN dim_card_type ct ON f.card_type_id = ct.card_type_id
JOIN dim_card_brand cb ON f.card_brand_id = cb.brand_id
GROUP BY ct.type_name, cb.brand_name
ORDER BY "Numero frodi" DESC, "Percentuale frodi" DESC;

-- 4. Pattern Temporali (Giorno della Settimana)
SELECT 
    TO_CHAR(transaction_date, 'Day') AS "Giorno settimana",
    COUNT(*) AS "Numero transazioni",
    SUM(is_fraud::int) AS "Numero frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions
GROUP BY TO_CHAR(transaction_date, 'Day'), DATE_PART('dow', transaction_date)
ORDER BY DATE_PART('dow', transaction_date);

-- 5. Correlazione tra Importo e Frode
SELECT 
    CASE 
        WHEN amount < 500 THEN '0-500'
        WHEN amount < 1000 THEN '500-1000'
        WHEN amount < 5000 THEN '1000-5000'
        ELSE '5000+'
    END AS "Fascia importo",
    COUNT(*) AS "Numero transazioni",
    SUM(is_fraud::int) AS "Numero frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions
GROUP BY "Fascia importo"
ORDER BY MIN(amount);

-- 6. Analisi per Provider Email
SELECT 
    e.provider_name AS "Provider email",
    COUNT(*) AS "Numero transazioni",
    SUM(is_fraud::int) AS "Numero frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions f
JOIN dim_email_provider e ON f.email_provider_id = e.provider_id
GROUP BY e.provider_name
ORDER BY "Percentuale frodi" DESC;

-- 7. Trend Mensile delle Transazioni
SELECT 
    TO_CHAR(transaction_date, 'MM-YYYY') AS "Mese",
    COUNT(*) AS "Numero transazioni",
    SUM(is_fraud::int) AS "Numero frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions
GROUP BY TO_CHAR(transaction_date, 'MM-YYYY')
ORDER BY "Percentuale frodi" DESC, "Numero frodi" DESC, "Mese";

-- 8. Combinazione Paese-Tipo di Prodotto
SELECT 
    c.country_name AS "Paese",
    p.product_name AS "Prodotto",
    COUNT(*) AS "Numero transazioni",
    SUM(is_fraud::int) AS "Numero frodi",
    ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY c.country_name, p.product_name
ORDER BY "Numero frodi" DESC, "Percentuale frodi" DESC
LIMIT 15;

-- 9. Individuazione Anomalie (Transazioni sospette)
SELECT 
    transaction_id AS "ID transazione",
    amount AS "Importo frode",
    transaction_date AS "Data transazione",
    c.country_name AS "Paese",
    p.product_name AS "Prodotto"
FROM fact_transactions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_product p ON f.product_id = p.product_id
WHERE 
    is_fraud AND
    amount > (SELECT AVG(amount) + 3 * STDDEV(amount) FROM fact_transactions WHERE is_fraud)
ORDER BY "Importo frode" DESC;

-- 10. Efficacia del Modello di Rilevamento
SELECT 
    (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud) AS "Numero frodi",
    (SELECT COUNT(*) FROM fact_transactions WHERE NOT is_fraud) AS "Transazioni legittime",
    ROUND(100.0 * (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud) / 
          (SELECT COUNT(*) FROM fact_transactions), 2) AS "Tasso frode";

-- 11. Paesi con più frodi (in valore assoluto e percentuale)
SELECT
    c.country_name AS "Paese",
    COUNT(*) AS "Transazioni totali",
    SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) AS "Frodi",
    ROUND(AVG(t.amount), 2) AS "Importo medio frode",
	ROUND(100.0 * AVG(is_fraud::int), 2) AS "Percentuale frodi"
FROM fact_transactions t
JOIN dim_country c ON t.country_id = c.country_id
GROUP BY c.country_name
ORDER BY "Frodi" DESC
LIMIT 5;
