-- TASK 1

SELECT country_code,
    COUNT(country_code) AS total_no_stores 
FROM dim_store_details 
GROUP BY
country_code


-- TASK 2

SELECT locality,
    COUNT(locality) AS total_no_stores 
FROM dim_store_details 
GROUP BY
locality
ORDER BY
total_no_stores DESC
LIMIT
7

-- TASK 3

SELECT dim_date_times.month, 
       ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity)AS NUMERIC),2) as total_sales
FROM dim_date_times
INNER JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC;

-- TASK 4

SELECT
    CASE
        WHEN orders_table.store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS location,
 COUNT(orders_table.store_code) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count
FROM dim_store_details
INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
GROUP BY location

-- TASK 5

SELECT store_type, 
    ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) AS total_sales,
    ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) / 
    (
        SELECT ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) 
        FROM dim_store_details
        INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
        INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
    )*100 AS percentage_total

FROM dim_store_details
INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY total_sales DESC

-- TASK 6

SELECT year, month, ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales
FROM dim_date_times
INNER JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY year, month
ORDER BY total_sales DESC

-- TASK 7

SELECT country_code, SUM(staff_numbers) AS total_staff_number
FROM dim_store_details
GROUP BY country_code

-- TASK 8

SELECT ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) as total_sales,store_type, country_code
FROM dim_store_details
INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code

-- TASK 9



SELECT * FROM dim_date_times
SELECT * FROM dim_users
SELECT * FROM dim_products
SELECT * FROM orders_table
SELECT * FROM dim_store_details
SELECT * FROM dim_card_details
