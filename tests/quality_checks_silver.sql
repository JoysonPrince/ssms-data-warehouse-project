/*
==================================================================================================================================
--> This script is a "DATA QUALITY CHECK" script intended to perform various data correctness checks across all 6 tables.
==================================================================================================================================
*/

/*
==============================================================
Check on Table #1: 'silver.crm_cust_info'
--> This table has 7 columns: 'cst_id', 'cst_key', 'cst_firstname', 'cst_lastname', 'cst_marital_status', 'cst_gndr' & 'cst_create_date'

Transformations done:
1. 'cst_id': PRIMARY KEY column. Has duplicates. Thus, kept the latest IDs partitioned by 'cst_id' & sorted by 'create_date' DESC
2. 'cst_key': NONE.
3. 'cst_firstname' & 'cst_lastname': Removed whitespaces
4. 'cst_gndr' & 'cst_marital_status': Data Normalization done on low-cardinality columns.
5. 'cst_create_date: NONE.
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

-- 3. Data quality check for low cardinality: Data Normalization
-- Check on 'cst_gndr' column
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


/*
==============================================================
Check on Table #2: 'silver.crm_prod_info'
--> This table has 6 columns: 'prd_id', 'prd_key', 'prd_nm', 'prd_cost', 'prd_line', 'prd_start_dt' & 'prd_end_dt'

Transformations done:
1. 'prd_id': NONE.
2. 'cst_key': The original 'prd_key' column split into 2 derived columns: 'cat_id' & 'prd_key'
3. 'prd_nm': NONE.
4. 'prd_cost': Replaced 0 values with NULL.
5. 'prd_line': Data Normalization done on low-cardinality column.
6. 'prd_start_dt': NONE. Casted as DATE from DATETIME.
7. 'prd_end_dt': --> Used LEAD() to get the next start date in the specified window - 1 DAY to get the new end date values.
                 --> Casted to DATE from DATETIME.
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
Check on Table #3: 'silver.crm_sales_details'
--> This table has 9 columns: 'sls_ord_num', 'sls_prd_key', 'sls_cust_id', 'sls_order_dt', 'sls_ship_dt',
 							  'sls_due_dt', 'sls_sales', 'sls_quantity', 'sls_price'

Transformations done:
1. 'sls_ord_num', 'sls_prd_key' & 'sls_cust_id': NONE.
2. 'sls_order_dt', 'sls_ship_dt', 'sls_due_dt': --> LEN(sls_order_dt) != 8 & 0 values transformed to NULL.
												--> Same follows for the other date columns.
3. 'sls_sales' & 'sls_price': Logic based transformations.
4. 'sls_quantity': NONE.
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


/*
==============================================================
Check on Table #4: 'silver.erp_cust_az12'
--> This table has 3 columns: 'cid', 'bdate', & 'gen'

Transformations done:
1. 'cid': Extracted string values from 'cid' to match 'cst_key' from 'silver.crm_cust_info'
2. 'bdate': Old birthdates kept as such, future birthdate values transformed to NULL
3. 'gen': Values normalized to match the standard. Eg: 'F' --> 'Female', 'M' --> 'Male', etc...
==============================================================
*/

-- Check if 'cid' has 'NAS%' as prefixes in its values:
SELECT *
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%';

-- For 'bdate' column, check out-of-range dates:
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1920-01-01' OR bdate > GETDATE();

-- Data standardization & consistency -- Check for 'gen' column:
SELECT DISTINCT gen
FROM silver.erp_cust_az12;


/*
==============================================================
Check on Table #5: 'silver.erp_loc_a101'
--> This table has 2 columns: 'cid' & 'cntry'

Transformations done:
1. 'cid': Removed '-' from 'cid' to match 'cst_key' from 'silver.crm_cust_info'
2. 'cntry': Old birthdates kept as such, future birthdate values transformed to NULL
==============================================================
*/

SELECT cid
FROM silver.erp_loc_a101
WHERE cid LIKE 'AW-%';
----------------
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;


/*
==============================================================
Check on Table #6: 'silver.erp_px_cat_g1v2'
--> This table has 2 columns: 'id', 'cat', 'subcat' & 'maintenance'

Transformations done:
1. 'id': NONE.
2. 'cat': NONE.
3. 'subcat': NONE.
4. 'maintenance': NONE.
==============================================================
*/

