USE DataWarehouse;

------------------------------
-- INSERT transformado
------------------------------
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) = '' THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry
FROM bronze.erp_loc_a101;

------------------------------
-- Verificação da tabela Silver
------------------------------
SELECT * 
FROM silver.erp_loc_a101;

------------------------------
-- Checagem de CID não correspondentes
------------------------------
SELECT 
    REPLACE(cid, '-', '') AS cid,
    LEN(cid),
    cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info);

------------------------------
-- Check de CIDs e tamanho
------------------------------
SELECT 
    cst_key,
    LEN(cst_key)
FROM silver.crm_cust_info;

------------------------------
-- Distinct para análise de consistência
------------------------------
SELECT DISTINCT
    cntry,
    CASE 
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(TRIM(REPLACE(cntry, CHAR(13), ''))) = '' THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry
FROM bronze.erp_loc_a101;
