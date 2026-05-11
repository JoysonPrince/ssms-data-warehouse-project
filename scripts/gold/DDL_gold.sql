/*
==================================================================
DDL Script: Create GOLD VIEWS:
==================================================================
Script purpose:
1. This script creates "views" for the Gold layer in the data warehouse.
2. The Gold layer represents the final DIMENSION and FACT Tables -- STAR SCHEMA.
3. Each view performs transformations and combines data from the Silver layer to produce clean, enriched and business-ready dataset.

Usage:
-- These views can be queried directly for analytics and reporting.
==================================================================
*/


-- ===============================================
-- Create DIMENSION TABLE #1: gold.dim_customers
-- ===============================================

GO
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
   DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers AS
SELECT  ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- SURROGATE KEY
        ci.cst_id AS customer_id,                               -- PRIMARY KEY
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name, 
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			 ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;



-- ===============================================
-- Create DIMENSION TABLE #2: gold.dim_products
-- ===============================================

GO
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
   DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,  -- SURROGATE KEY
	   pn.prd_id AS product_id,                                                  -- PRIMARY KEY
	   pn.prd_key AS product_number,                                             
	   pn.prd_nm AS product_name,
	   pn.prd_line AS product_line,
       pn.cat_id AS category_id,
	   pc.cat AS category, 
	   pc.subcat AS subcategory,
	   pc.maintenance,
	   pn.prd_cost AS cost, 
	   pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;



-- ===============================================
-- Create FACT TABLE: gold.dim_products
-- ===============================================

GO
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
   DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT sls_ord_num AS order_number,	
	   dp.product_key, 
	   dc.customer_key, 
	   sls_order_dt AS order_date, 
	   sls_ship_dt AS shipping_date, 
	   sls_due_dt AS due_date, 
	   sls_sales AS sales_amount, 
	   sls_quantity AS quantity, 
	   sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp ON sd.sls_prd_key = dp.product_number;

--- THE END ---
