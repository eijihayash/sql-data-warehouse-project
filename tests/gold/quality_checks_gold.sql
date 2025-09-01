-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- Check dimension customers
SELECT * FROM gold.dim_customers;

-- Check distinct values for gender in customers
SELECT DISTINCT gen FROM gold.dim_customers;

-- Check raw data sources
SELECT * FROM silver.erp_cust_az12;
SELECT * FROM silver.erp_loc_a101;


-- ============================================
-- DUPLICATES CHECK - CUSTOMERS
-- ============================================
SELECT t.cst_id , COUNT(*) 
FROM (
	SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_material_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;


-- ============================================
-- GENDER STANDARDIZATION
-- ============================================
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;


-- ============================================
-- PRODUCTS CHECKS
-- ============================================

-- Check dimension products
SELECT * FROM gold.dim_products;

-- Check duplicates in products
SELECT prd_key, COUNT(*) 
FROM (
	SELECT 
		pi.prd_id,
		pi.cat_id,
		pi.prd_key,
		pi.prd_nm,
		pi.prd_cost,
		pi.prd_line,
		pi.prd_start_dt,
		pi.prd_end_dt,
		eg.cat,
		eg.subcat,
		eg.maintenance
	FROM silver.crm_prd_info pi
	LEFT JOIN silver.erp_px_cat_g1v2 eg
		ON pi.cat_id = eg.id
	WHERE pi.prd_end_dt IS NULL -- filter historical data
) t 
GROUP BY t.prd_key 
HAVING COUNT(*) > 1;

-- Check product categories
SELECT * FROM silver.erp_px_cat_g1v2;


-- ============================================
-- FOREIGN KEY INTEGRITY CHECK - FACT TABLES
-- ============================================
SELECT 
	*
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers cu
	ON fs.customer_id = cu.customer_id
LEFT JOIN gold.dim_products pr
	ON fs.product_key = pr.product_number
WHERE fs.customer_id IS NULL 
   OR fs.product_key IS NULL;
