USE DataWarehouse;
GO

/* ======================================================
   VALIDATION - DUPLICATES & NULL KEYS
   ====================================================== */

-- Bronze: Check for duplicates or NULL IDs
SELECT 
    cst_id,
    COUNT(*) AS total
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Silver: Check for duplicates or NULL IDs
SELECT 
    cst_id,
    COUNT(*) AS total
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO


/* ======================================================
   INSERT TRANSFORMED DATA INTO SILVER
   ====================================================== */
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_material_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS t
WHERE flag_last = 1;
GO


/* ======================================================
   DATA QUALITY CHECKS - TRIM UNWANTED SPACES
   Expectation: No Results
   ====================================================== */

-- Firstname
SELECT cst_firstname FROM bronze.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);

-- Lastname
SELECT cst_lastname FROM bronze.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);
SELECT cst_lastname FROM silver.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);

-- Material Status
SELECT cst_material_status FROM bronze.crm_cust_info WHERE cst_material_status != TRIM(cst_material_status);
SELECT cst_material_status FROM silver.crm_cust_info WHERE cst_material_status != TRIM(cst_material_status);

-- Gender
SELECT cst_gndr FROM bronze.crm_cust_info WHERE cst_gndr != TRIM(cst_gndr);
SELECT cst_gndr FROM silver.crm_cust_info WHERE cst_gndr != TRIM(cst_gndr);
GO


/* ======================================================
   DATA STANDARDIZATION & CONSISTENCY
   ====================================================== */

-- Distinct Gender Values
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;

-- Distinct Material Status Values
SELECT DISTINCT cst_material_status FROM bronze.crm_cust_info;
SELECT DISTINCT cst_material_status FROM silver.crm_cust_info;
GO


/* ======================================================
   FINAL CHECK - DATA IN SILVER
   ====================================================== */
SELECT * 
FROM silver.crm_cust_info;
GO
