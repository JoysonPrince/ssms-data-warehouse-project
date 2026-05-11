/*
===================================================================
Quality checks: 
===================================================================
Script purpose:
1. This script performs quality checks to validate the integrity, consistency and accuracy of the Gold layer.
2. These checks ensure:
   -- UNIQUENESS of SURROGATE KEYS in the DIMENSION tables.
   -- Referential integrity between FACT and DIMENSION tables.
   -- Validation of relationships in the data model for analytical purposes.

Usage notes:
-- Run these checks after data loading Silver Layer.
-- Investigate and resolve any discrepancies found during the checks.
====================================================================
*/

/*
=============================================================
Quality check for gold.dim_customers
=============================================================
*/

-- Data integration
SELECT DISTINCT gender FROM gold.dim_customers;
-- Expected result: 'n/a', 'Male' & 'Female'

-- Check for uniqueness of the 'customer_key'
SELECT customer_key, COUNT(*) AS customer_key_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
-- Expected result: Blank



/*
=============================================================
Quality check for gold.dim_products
=============================================================
*/

-- Check for uniqueness of the 'product_key'
SELECT product_key, COUNT(*) AS product_key_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
-- Expected result: Blank



/*
=============================================================
Quality check for gold.fact_sales
=============================================================
*/

-- Checking Data connectivity between FACT and DIMENSIONS
SELECT *
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_products dp 
ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL OR dc.customer_key IS NULL;
