-- 1. Перевірка дублікатів PK у core (очікується 0 рядків)
SELECT 'customers' AS tbl, customer_id AS pk, COUNT(*) FROM core.customers GROUP BY 2 HAVING COUNT(*) > 1
UNION ALL
SELECT 'merchants', merchant_id, COUNT(*) FROM core.merchants GROUP BY 2 HAVING COUNT(*) > 1
UNION ALL
SELECT 'products', product_id, COUNT(*) FROM core.products GROUP BY 2 HAVING COUNT(*) > 1
UNION ALL
SELECT 'orders', order_id, COUNT(*) FROM core.orders GROUP BY 2 HAVING COUNT(*) > 1
UNION ALL
SELECT 'order_items', order_item_id, COUNT(*) FROM core.order_items GROUP BY 2 HAVING COUNT(*) > 1;

-- 2. Перевірка відсутніх FK у core (очікується 0 рядків)
SELECT 'products->merchants' AS rel, COUNT(*) AS missing_fk
FROM core.products p LEFT JOIN core.merchants m ON p.merchant_id = m.merchant_id WHERE m.merchant_id IS NULL
UNION ALL
SELECT 'orders->customers', COUNT(*)
FROM core.orders o LEFT JOIN core.customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL
UNION ALL
SELECT 'orders->merchants', COUNT(*)
FROM core.orders o LEFT JOIN core.merchants m ON o.merchant_id = m.merchant_id WHERE m.merchant_id IS NULL
UNION ALL
SELECT 'order_items->orders', COUNT(*)
FROM core.order_items oi LEFT JOIN core.orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL
UNION ALL
SELECT 'order_items->products', COUNT(*)
FROM core.order_items oi LEFT JOIN core.products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL;

-- 3. Звірка кількості та сум за валютами між accepted (staging+audit) і core
WITH stg_acc_orders AS (
    SELECT btrim(s.currency) AS currency, COUNT(*) AS cnt, SUM(s.total_amount::numeric(12,2)) AS sum_amt
    FROM stg.orders s
    JOIN audit.row_decisions d ON d.source_file = 'orders.csv' AND d.source_row_number = s.source_row_number
    WHERE d.run_id = 1 AND d.status = 'accepted'
    GROUP BY 1
),
core_orders AS (
    SELECT currency, COUNT(*) AS cnt, SUM(total_amount) AS sum_amt
    FROM core.orders GROUP BY 1
)
SELECT 'orders' AS entity, a.currency, a.cnt AS accepted_cnt, c.cnt AS core_cnt, a.sum_amt AS accepted_sum, c.sum_amt AS core_sum
FROM stg_acc_orders a FULL JOIN core_orders c ON a.currency = c.currency
ORDER BY 2;

WITH stg_acc_items AS (
    SELECT btrim(s.currency) AS currency, COUNT(*) AS cnt, SUM(s.line_total::numeric(12,2)) AS sum_amt
    FROM stg.order_items s
    JOIN audit.row_decisions d ON d.source_file = 'order_items.csv' AND d.source_row_number = s.source_row_number
    WHERE d.run_id = 1 AND d.status = 'accepted'
    GROUP BY 1
),
core_items AS (
    SELECT currency, COUNT(*) AS cnt, SUM(line_total) AS sum_amt
    FROM core.order_items GROUP BY 1
)
SELECT 'order_items' AS entity, a.currency, a.cnt AS accepted_cnt, c.cnt AS core_cnt, a.sum_amt AS accepted_sum, c.sum_amt AS core_sum
FROM stg_acc_items a FULL JOIN core_items c ON a.currency = c.currency
ORDER BY 2;

-- 4. Порівняння помилкового SUM після JOIN та правильного запиту (grain = замовлення)
SELECT 'inflated_join_sum' AS method, o.currency, SUM(o.total_amount) AS total_sum
FROM core.orders o
JOIN core.order_items oi ON o.order_id = oi.order_id
GROUP BY o.currency
UNION ALL
SELECT 'correct_orders_sum' AS method, currency, SUM(total_amount) AS total_sum
FROM core.orders
GROUP BY currency
ORDER BY 2, 1;