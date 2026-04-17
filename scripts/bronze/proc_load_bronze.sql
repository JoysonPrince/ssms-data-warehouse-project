/*
===========================================================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze layer)
===========================================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from the external csv files.
It performs the following actions:
1. Truncates the 'bronze' tables before loading the data,
2. Uses the 'BULK INSERT' command to load data from csv files into the bronze tables.

Parameters: None
This stored procedure does not accept any parameter nor returns any values.

Usage example:
EXEC bronze.load_bronze;
============================================================================================
*/

GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Joyson\DwB\DWH Project-JPA\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		( FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>> Inserting Data into: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Joyson\DwB\DWH Project-JPA\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		( FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Joyson\DwB\DWH Project-JPA\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		( FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		PRINT '------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Joyson\DwB\DWH Project-JPA\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH
		( FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Joyson\DwB\DWH Project-JPA\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH
		( FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Joyson\DwB\DWH Project-JPA\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH
		( FIRSTROW = 2,
		  FIELDTERMINATOR = ',',
		  TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------';

		SET @batch_end_time = GETDATE();
		PRINT '================================';
		PRINT 'Loading Bronze Layer is complete';
		PRINT '-- Total load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================';
	END TRY
	BEGIN CATCH
				PRINT '=====================================';
				PRINT 'Error occurred during loading the Bronze Layer';
				PRINT 'Error message: ' + ERROR_MESSAGE();
				PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR);
				PRINT '=====================================';
	END CATCH
END
