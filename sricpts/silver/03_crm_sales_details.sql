USE DataWarehouse;

--------------------------------------
-- 1. Insert into silver.crm_sales_details
--------------------------------------
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity,0)
         ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_detail;


--------------------------------------
-- 2. Check inserted data
--------------------------------------
SELECT *
FROM silver.crm_sales_details;


--------------------------------------
-- 3. Check for unwanted spaces
--------------------------------------
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_detail
WHERE sls_ord_num != TRIM(sls_ord_num);


--------------------------------------
-- 4. Referential integrity checks
--------------------------------------
-- Products
SELECT *
FROM bronze.crm_sales_detail
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Customers
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity,0)
         ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_detail
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);


--------------------------------------
-- 5. Date validation
--------------------------------------
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_detail
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101 
   OR sls_order_dt < 19000101;

SELECT sls_ship_dt
FROM bronze.crm_sales_detail
WHERE sls_ship_dt <= 0
   OR LEN(sls_ship_dt) != 8;

SELECT sls_due_dt
FROM bronze.crm_sales_detail
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8;


--------------------------------------
-- 6. Check order date consistency
--------------------------------------
SELECT *
FROM bronze.crm_sales_detail
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


--------------------------------------
-- 7. Check sales calculation consistency
--------------------------------------
SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity,0)
         ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_detail
WHERE sls_sales != (sls_quantity * sls_price)
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;
