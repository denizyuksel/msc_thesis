WITH
user_transactions AS (
    SELECT DISTINCT
        DATE(from_unixtime(cast(timestamp as double) / 1000)) AS transaction_date,
        from_hex(CAST(json_extract(transactions, '$[0].hash') AS VARCHAR)) AS user_tx
    FROM 
        mevblocker.raw_bundles
    WHERE 
        DATE(from_unixtime(cast(timestamp as double) / 1000)) BETWEEN CAST('2023-04-01' AS DATE) AND CAST('2024-03-23' AS DATE)
        AND HOUR(from_unixtime(cast(timestamp as double) / 1000)) = 18
),
mined_transactions AS (
    SELECT 
        user_transactions.transaction_date,
        user_transactions.user_tx
    FROM 
        user_transactions
    INNER JOIN 
        ethereum.transactions ON user_transactions.user_tx = ethereum.transactions.hash
),
transaction_counts AS (
    SELECT 
        transaction_date,
        COUNT(*) AS total_transactions
    FROM 
        user_transactions
    GROUP BY 
        transaction_date
),
mined_counts AS (
    SELECT 
        transaction_date,
        COUNT(*) AS mined_transactions
    FROM 
        mined_transactions
    GROUP BY 
        transaction_date
)
SELECT
    transaction_counts.transaction_date,
    transaction_counts.total_transactions,
    COALESCE(mined_counts.mined_transactions, 0) AS mined,
    transaction_counts.total_transactions - COALESCE(mined_counts.mined_transactions, 0) AS unmined
FROM 
    transaction_counts
LEFT JOIN 
    mined_counts ON transaction_counts.transaction_date = mined_counts.transaction_date
ORDER BY 
    transaction_counts.transaction_date;
