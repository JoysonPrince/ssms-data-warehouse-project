
/*
==============================================================
Checking 'silver.crm_cust_info'
==============================================================
*/

-- 1. Data quality check for NULLS or duplicates in the PRIMARY KEY column --> 'cst_id' :
SELECT cst_id, COUNT(*) XYZ
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2. Data quality check for whitespaces:
-- Check on 'cst_firstname' column
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data quality check on 'cst_lastname' column
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- 3. Data quality check for low cardinality:
-- Check on 'cst_gndr' column
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


/*
==============================================================
Checking 'silver.crm_prod_info'
==============================================================
*/

-- 1. Data quality check on 'prd_cost':
-- Check for NULLS & negative numbers in a INT based column
SELECT prd_cost
FROM silver.crm_prod_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 2. Data quality check on 'cat_id' & 'prd_key' columns:
-- The original 'prd_key' was split into 2 derived columns.
SELECT cat_id, prd_key
FROM silver.crm_prod_info;

-- 3. Data quality check on 'prd_line':
-- 'prd_line' is a low cardinality column
SELECT DISTINCT prd_line FROM silver.crm_prod_info;

-- 4. Data quality check on invalid Date orders:
-- End dates must not be earlier than start dates !!!	
SELECT * FROM silver.crm_prod_info
WHERE prd_end_dt < prd_start_dt;


/*
==============================================================
Checking 'silver.crm_sales_details'
==============================================================
*/

-- No transformations are needed for the first 3 columns:- 'sls_ord_num', 'sls_prd_key' & 'sls_cust_id'. So no data quality checks.

-- Data quality check for invalid date orders:
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Data quality check for calculations:
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
											OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;
