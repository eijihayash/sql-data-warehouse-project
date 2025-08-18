/*
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Trincates the bronze tables before loading data.
- Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.
*/


USE DataWarehouse;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @bach_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==================================================';
		PRINT'Loading Bronze Layer';
		PRINT '==================================================';
		
		
		PRINT '--------------------------------------------------';
		PRINT'Loading CRM TABLE';
		PRINT '--------------------------------------------------';
		
		SET @start_time = GETDATE();
	-- inserting data into the Docker container because the file is not accessible from the host machine
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM '/var/opt/mssql/data/source_crm/cust_info.csv'
		WITH (
	    	FIRSTROW = 2,
	    	FIELDTERMINATOR = ',',
	    	TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' : seconds'; 
		PRINT'------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM '/var/opt/mssql/data/source_crm/prd_info.csv'
		WITH (
	    	FIRSTROW = 2,
	    	FIELDTERMINATOR = ',',
	    	TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT'------------------------------------------------------';

		SET @end_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_sales_detail;
		
		PRINT '>> Inserting Data Into Table: bronze.crm_sales_detail';
		BULK INSERT bronze.crm_sales_detail
		FROM '/var/opt/mssql/data/source_crm/sales_details.csv'
		WITH (
		    FIRSTROW = 2,
		    FIELDTERMINATOR = ',',
		    TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
	
		
		PRINT '--------------------------------------------------';
		PRINT'Loading ERP TABLE';
		PRINT '--------------------------------------------------';
		
		SET @end_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM '/var/opt/mssql/data/source_erp/CUST_AZ12.csv'
		WITH (
		    FIRSTROW = 2,
		    FIELDTERMINATOR = ',',
		    TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT'------------------------------------------------------';
		
		SET @end_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		
		PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '/var/opt/mssql/data/source_erp/LOC_A101.csv'
		WITH (
		    FIRSTROW = 2,
		    FIELDTERMINATOR = ',',
		    TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT'------------------------------------------------------';
		
		SET @end_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '/var/opt/mssql/data/source_erp/PX_CAT_G1V2.csv'
		WITH (
		    FIRSTROW = 2,
		    FIELDTERMINATOR = ',',
		    TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT'------------------------------------------------------';
		
		
		SET @bach_end_time = GETDATE();
		PRINT '======================================================'
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @bach_end_time) AS NVARCHAR) + ' seconds';
		PRINT '======================================================'
	END TRY
	BEGIN CATCH
		PRINT '========================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Masage' + ERROR_MESSAGE();
		PRINT 'Error Masage' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Masage' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================================='
	END CATCH
END

EXEC bronze.load_bronze;

SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_detail;
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT COUNT(*) FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_sales_detail;
SELECT COUNT(*) FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
