USE DataWarehouse;

------------------------------------------------------------
-- INSERT CLEANED DATA INTO SILVER
------------------------------------------------------------
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT 
    -- Limpa o CID (remove prefixo NAS se existir)
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    -- Valida datas de nascimento
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    -- Normaliza gênero
    CASE
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE')   THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


------------------------------------------------------------
-- CHECKS BÁSICOS DE CARGA
------------------------------------------------------------
-- Verifica dados carregados
SELECT * FROM silver.erp_cust_az12;

-- Verifica tabela de clientes já existente
SELECT * FROM silver.crm_cust_info;


------------------------------------------------------------
-- CHECKS DE CHAVES (CID)
------------------------------------------------------------
-- CIDs transformados que não encontram match em crm_cust_info
SELECT 
    cid AS cid_old,
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid
FROM bronze.erp_cust_az12
WHERE CASE 
          WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
          ELSE cid
      END NOT IN (SELECT cst_key FROM silver.crm_cust_info);


------------------------------------------------------------
-- CHECKS DE DATAS
------------------------------------------------------------
-- Datas de nascimento inválidas (antes de 1924 ou depois de hoje)
SELECT DISTINCT 
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


------------------------------------------------------------
-- CHECKS DE GÊNERO
------------------------------------------------------------
-- Verifica caracteres estranhos e tamanhos
SELECT DISTINCT 
    gen,
    LEN(gen) AS length,
    ASCII(SUBSTRING(gen, 1, 1)) AS ascii_first,
    ASCII(SUBSTRING(gen, LEN(gen), 1)) AS ascii_last
FROM bronze.erp_cust_az12;

-- Normalização do campo gen
SELECT DISTINCT 
    gen,
    CASE
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE')   THEN 'Male'
        ELSE 'n/a'
    END AS gen_norm
FROM bronze.erp_cust_az12;
