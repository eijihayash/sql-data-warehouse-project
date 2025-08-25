USE DataWarehouse;

------------------------------
-- INSERT transformado
------------------------------
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT 
    id,
    cat,
    subcat,
    CASE 
        WHEN TRIM(REPLACE(maintenance, CHAR(13), '')) = 'Yes' THEN 'Yes'
        ELSE maintenance
    END AS maintenance
FROM bronze.erp_px_cat_g1v2;

------------------------------
-- Verificação da tabela carregada
------------------------------
SELECT * FROM silver.erp_px_cat_g1v2;

------------------------------
-- Check de espaços ou inconsistências
------------------------------
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

------------------------------
-- Distinct para análise de consistência
------------------------------
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
    CASE 
        WHEN TRIM(REPLACE(maintenance, CHAR(13), '')) = 'Yes' THEN 'Yes'
        ELSE maintenance
    END AS maintenance
FROM bronze.erp_px_cat_g1v2;
