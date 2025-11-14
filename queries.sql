-- ============================================================================
-- E-Commerce Analytics SQL Queries
-- Database: db/ecommerce.db
-- ============================================================================

-- Query 1: Top 10 customers by total spend (last 12 months)
-- Purpose: Identify highest-value customers in the past year
-- ============================================================================
EXPLAIN QUERY PLAN
SELECT 
    c.customer_id,
    c.name,
    c.email,
    c.country,
    ROUND(SUM(o.total_amount), 2) as total_spend,
    COUNT(DISTINCT o.order_id) as order_count
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= date('now', '-12 months')
    AND o.status IN ('paid', 'shipped')
GROUP BY c.customer_id, c.name, c.email, c.country
ORDER BY total_spend DESC
LIMIT 10;

-- Example output:
-- customer_id | name          | email              | country | total_spend | order_count
-- ------------|---------------|--------------------|---------|-------------|-------------
-- uuid-123    | John Smith    | john@example.com   | USA     | 15432.50    | 12
-- uuid-456    | Jane Doe      | jane@example.com   | UK      | 12890.25    | 8
-- uuid-789    | Bob Johnson   | bob@example.com    | Canada  | 11234.75    | 15


-- ============================================================================
-- Query 2: Monthly revenue by product category for the last 6 months
-- Purpose: Track category performance trends over time
-- ============================================================================
EXPLAIN QUERY PLAN
WITH monthly_category_revenue AS (
    SELECT 
        strftime('%Y-%m', o.order_date) as month,
        p.category,
        SUM(oi.quantity * oi.unit_price) as revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_date >= date('now', '-6 months')
        AND o.status IN ('paid', 'shipped')
    GROUP BY month, p.category
)
SELECT 
    month,
    category,
    ROUND(revenue, 2) as revenue
FROM monthly_category_revenue
ORDER BY month DESC, revenue DESC;

-- Example output:
-- month    | category    | revenue
-- ---------|-------------|----------
-- 2024-06  | electronics | 45230.50
-- 2024-06  | clothing    | 32100.25
-- 2024-06  | home        | 28900.75
-- 2024-05  | electronics | 43890.00
-- 2024-05  | clothing    | 31200.50
-- 2024-05  | home        | 27500.25


-- ============================================================================
-- Query 3: Most frequently purchased product pairs (products bought together)
-- Purpose: Identify cross-selling opportunities
-- ============================================================================
EXPLAIN QUERY PLAN
WITH order_product_pairs AS (
    SELECT DISTINCT
        oi1.order_id,
        oi1.product_id as product1_id,
        oi2.product_id as product2_id,
        p1.name as product1_name,
        p2.name as product2_name
    FROM order_items oi1
    INNER JOIN order_items oi2 ON oi1.order_id = oi2.order_id
        AND oi1.product_id < oi2.product_id
    INNER JOIN products p1 ON oi1.product_id = p1.product_id
    INNER JOIN products p2 ON oi2.product_id = p2.product_id
    INNER JOIN orders o ON oi1.order_id = o.order_id
    WHERE o.status IN ('paid', 'shipped')
)
SELECT 
    product1_name,
    product2_name,
    COUNT(*) as times_bought_together
FROM order_product_pairs
GROUP BY product1_id, product2_id, product1_name, product2_name
ORDER BY times_bought_together DESC
LIMIT 10;

-- Example output:
-- product1_name              | product2_name              | times_bought_together
-- ---------------------------|----------------------------|----------------------
-- Premium Electronics Item   | Classic Home Item          | 45
-- Designer Clothing Item     | Sport Clothing Item        | 38
-- Smart Electronics Item     | Modern Home Item           | 32
-- Natural Beauty Item         | Organic Beauty Item        | 28
-- Professional Sports Item    | Training Sports Item       | 25


-- ============================================================================
-- Query 4: Average rating per product and top 10 highest-rated products (min 10 reviews)
-- Purpose: Identify best-performing products by customer satisfaction
-- ============================================================================
EXPLAIN QUERY PLAN
WITH product_ratings AS (
    SELECT 
        p.product_id,
        p.name,
        p.category,
        COUNT(r.review_id) as review_count,
        ROUND(AVG(r.rating), 2) as avg_rating
    FROM products p
    LEFT JOIN reviews r ON p.product_id = r.product_id
    GROUP BY p.product_id, p.name, p.category
    HAVING COUNT(r.review_id) >= 10
)
SELECT 
    product_id,
    name,
    category,
    review_count,
    avg_rating
FROM product_ratings
ORDER BY avg_rating DESC, review_count DESC
LIMIT 10;

-- Example output:
-- product_id | name                  | category    | review_count | avg_rating
-- -----------|-----------------------|-------------|--------------|------------
-- 42         | Premium Electronics   | electronics | 45           | 4.85
-- 78         | Luxury Beauty Item    | beauty      | 32           | 4.82
-- 15         | Designer Clothing     | clothing    | 28           | 4.79
-- 91         | Modern Home Item      | home        | 35           | 4.76
-- 56         | Professional Sports   | sports      | 22           | 4.73


-- ============================================================================
-- Query 5: Customer retention - number of customers who placed >1 order and 
--          repeat purchase rate by cohort month
-- Purpose: Measure customer loyalty and retention by signup cohort
-- ============================================================================
EXPLAIN QUERY PLAN
WITH customer_cohorts AS (
    SELECT 
        c.customer_id,
        strftime('%Y-%m', c.signup_date) as cohort_month,
        COUNT(DISTINCT o.order_id) as order_count
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
        AND o.status IN ('paid', 'shipped')
    GROUP BY c.customer_id, cohort_month
),
cohort_stats AS (
    SELECT 
        cohort_month,
        COUNT(*) as total_customers,
        SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) as repeat_customers
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    cohort_month,
    total_customers,
    repeat_customers,
    ROUND(100.0 * repeat_customers / total_customers, 2) as repeat_rate_pct
FROM cohort_stats
ORDER BY cohort_month DESC;

-- Example output:
-- cohort_month | total_customers | repeat_customers | repeat_rate_pct
-- -------------|-----------------|------------------|-----------------
-- 2024-06      | 45              | 18               | 40.00
-- 2024-05      | 52              | 22               | 42.31
-- 2024-04      | 48              | 21               | 43.75
-- 2024-03      | 55              | 25               | 45.45
-- 2024-02      | 50              | 23               | 46.00


-- ============================================================================
-- Query 6: Orders with mismatched totals (to validate ingestion)
-- Purpose: Data quality check - find orders where total doesn't match order_items sum
-- ============================================================================
EXPLAIN QUERY PLAN
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount as order_total,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) as calculated_total,
    ROUND(ABS(o.total_amount - SUM(oi.quantity * oi.unit_price)), 2) as difference
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.customer_id, o.order_date, o.total_amount
HAVING ABS(o.total_amount - SUM(oi.quantity * oi.unit_price)) > 0.01
ORDER BY difference DESC;

-- Example output:
-- order_id | customer_id | order_date  | order_total | calculated_total | difference
-- ---------|-------------|-------------|-------------|------------------|------------
-- (Should be empty if data is correct, or show mismatches if any exist)


-- ============================================================================
-- Query 7: Products with highest return rate
-- Purpose: Identify products that customers frequently return
-- ============================================================================
EXPLAIN QUERY PLAN
WITH product_returns AS (
    SELECT 
        p.product_id,
        p.name,
        p.category,
        COUNT(DISTINCT CASE WHEN o.status = 'returned' THEN oi.order_id END) as returned_orders,
        COUNT(DISTINCT oi.order_id) as total_orders
    FROM products p
    INNER JOIN order_items oi ON p.product_id = oi.product_id
    INNER JOIN orders o ON oi.order_id = o.order_id
    GROUP BY p.product_id, p.name, p.category
    HAVING total_orders > 0
)
SELECT 
    product_id,
    name,
    category,
    total_orders,
    returned_orders,
    ROUND(100.0 * returned_orders / total_orders, 2) as return_rate_pct
FROM product_returns
WHERE returned_orders > 0
ORDER BY return_rate_pct DESC, returned_orders DESC
LIMIT 10;

-- Example output:
-- product_id | name              | category    | total_orders | returned_orders | return_rate_pct
-- -----------|-------------------|-------------|--------------|-----------------|----------------
-- 123        | Electronics Item  | electronics | 45           | 8               | 17.78
-- 67         | Clothing Item     | clothing    | 32           | 5               | 15.63
-- 89         | Home Item         | home        | 28           | 4               | 14.29
-- 45         | Beauty Item       | beauty      | 35           | 4               | 11.43
-- 156        | Sports Item       | sports      | 22           | 2               | 9.09


-- ============================================================================
-- Query 8: Average order value (AOV) by customer segment (is_premium true/false)
-- Purpose: Compare spending behavior between premium and regular customers
-- ============================================================================
EXPLAIN QUERY PLAN
SELECT 
    CASE 
        WHEN c.is_premium = 'True' THEN 'Premium'
        ELSE 'Regular'
    END as customer_segment,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    ROUND(AVG(o.total_amount), 2) as avg_order_value,
    ROUND(SUM(o.total_amount), 2) as total_revenue
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY customer_segment
ORDER BY avg_order_value DESC;

-- Example output:
-- customer_segment | total_orders | unique_customers | avg_order_value | total_revenue
-- -----------------|--------------|------------------|-----------------|---------------
-- Premium          | 450          | 125              | 245.50          | 110475.00
-- Regular          | 1050         | 375              | 185.25          | 194512.50


-- ============================================================================
-- Query 9: Fastest growing category (YoY percent change)
-- Purpose: Identify categories with strongest growth year-over-year
-- ============================================================================
EXPLAIN QUERY PLAN
WITH category_revenue_by_year AS (
    SELECT 
        p.category,
        strftime('%Y', o.order_date) as year,
        SUM(oi.quantity * oi.unit_price) as revenue
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    WHERE o.status IN ('paid', 'shipped')
        AND o.order_date >= date('now', '-2 years')
    GROUP BY p.category, year
),
year_comparison AS (
    SELECT 
        category,
        MAX(CASE WHEN year = strftime('%Y', date('now')) THEN revenue END) as current_year_revenue,
        MAX(CASE WHEN year = strftime('%Y', date('now', '-1 year')) THEN revenue END) as previous_year_revenue
    FROM category_revenue_by_year
    GROUP BY category
    HAVING previous_year_revenue IS NOT NULL AND previous_year_revenue > 0
)
SELECT 
    category,
    ROUND(current_year_revenue, 2) as current_year_revenue,
    ROUND(previous_year_revenue, 2) as previous_year_revenue,
    ROUND(100.0 * (current_year_revenue - previous_year_revenue) / previous_year_revenue, 2) as yoy_growth_pct
FROM year_comparison
ORDER BY yoy_growth_pct DESC
LIMIT 10;

-- Example output:
-- category    | current_year_revenue | previous_year_revenue | yoy_growth_pct
-- ------------|----------------------|----------------------|---------------
-- electronics | 125430.50            | 98500.25             | 27.34
-- clothing     | 98750.75             | 82300.50             | 20.00
-- beauty      | 76500.25             | 65200.00             | 17.33
-- home        | 89200.50             | 78500.75             | 13.64
-- sports      | 54300.25             | 48500.50             | 11.96


-- ============================================================================
-- Query 10: Top 10 orders with customer name, product names aggregated as comma list,
--           order total, and order_date (joins 4+ tables)
-- Purpose: Comprehensive order view with customer and product details
-- ============================================================================
EXPLAIN QUERY PLAN
WITH order_product_names AS (
    SELECT 
        oi.order_id,
        GROUP_CONCAT(p.name, ', ') as product_names,
        COUNT(DISTINCT oi.product_id) as product_count
    FROM order_items oi
    INNER JOIN products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
)
SELECT 
    o.order_id,
    c.name as customer_name,
    c.email as customer_email,
    c.country as customer_country,
    opn.product_names,
    opn.product_count,
    o.order_date,
    o.status,
    ROUND(o.total_amount, 2) as order_total
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_product_names opn ON o.order_id = opn.order_id
WHERE o.status IN ('paid', 'shipped')
ORDER BY o.total_amount DESC
LIMIT 10;

-- Example output:
-- order_id | customer_name | customer_email      | customer_country | product_names                    | product_count | order_date  | status | order_total
-- ---------|---------------|--------------------|------------------|----------------------------------|---------------|-------------|--------|-------------
-- 1234     | John Smith    | john@example.com   | USA              | Premium Electronics, Modern Home | 2             | 2024-06-15  | shipped| 1250.50
-- 5678     | Jane Doe      | jane@example.com   | UK               | Designer Clothing, Luxury Beauty | 2             | 2024-06-10  | paid   | 980.75
-- 9012     | Bob Johnson   | bob@example.com    | Canada           | Smart Electronics, Classic Home  | 2             | 2024-06-05  | shipped| 875.25
-- 3456     | Alice Brown   | alice@example.com  | Australia        | Professional Sports, Training    | 2             | 2024-05-28  | paid   | 750.00
-- 7890     | Charlie Davis | charlie@example.com| Germany          | Premium Electronics, Wireless    | 2             | 2024-05-20  | shipped| 695.50


