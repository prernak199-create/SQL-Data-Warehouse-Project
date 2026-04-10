/*
===============================================================================================
Stored Procedure:bronze.load_bronze (Source ->Bronze)
===============================================================================================
This procedure load data from external CSV files.
It truncate the Bronze Table and than use 'Bulk Insert' to load CSV files to bronze layer.
USE [DataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze]
Here it is Modified 
Script Date: 10-04-2026 20:24:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
================================================================================================*/
ALTER   PROCEDURE [bronze].[load_bronze]
AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time=GETDATE();
PRINT '===============================================';
PRINT 'Loading Bronze layer';
PRINT '===============================================';

PRINT '------------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '------------------------------------------------';

SET @start_time=GETDATE();
PRINT '>>Truncating table:bronze.crm_cust_info';
PRINT '>>Inserting into table:bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
from 'C:\sqldata\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
firstrow= 2,
fieldterminator =',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>Load Duration: ' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'second';
PRINT '------------------------------------------------------';


SET @start_time=GETDATE();
PRINT '>>Truncating table:bronze.crm_prd_info';
PRINT '>>Inserting into table:bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
from 'C:\sqldata\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
); 
SET @end_time=GETDATE();
PRINT '>>Load Duration: ' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'second';
PRINT '------------------------------------------------------';


SET @start_time=GETDATE();
PRINT '>>Truncating table:bronze.crm_sales_detail';
PRINT '>>Inserting into table:bronze.crm_sales_detail';
Truncate table bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
from 'C:\sqldata\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>Load Duration: ' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'second';
PRINT '------------------------------------------------------';


PRINT '------------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '------------------------------------------------';


SET @start_time=GETDATE();
PRINT '>>Truncating table:bronze.erp_cust_az12';
PRINT '>>Inserting into table:bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
from 'C:\sqldata\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>Load Duration: ' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'second';
PRINT '------------------------------------------------------';


SET @start_time=GETDATE();
PRINT '>>Truncating table:bronze.erp_loc_a101';
PRINT '>>Inserting into table:bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
from 'C:\sqldata\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>Load Duration: ' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'second';
PRINT '------------------------------------------------------';


SET @start_time=GETDATE();
PRINT '>>Truncating table:bronze.erp_px_cat_g1v2';
PRINT '>>Inserting into table:bronze.erp_pc_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
from 'C:\sqldata\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>Load Duration: ' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'second';
PRINT '------------------------------------------------------';

SET @batch_end_time=GETDATE();
PRINT '========================================================';
PRINT 'Loading Bronze Layer is Completed';
PRINT '>>Total Load Duration: ' +CAST(datediff(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +'second';
PRINT '========================================================';
END TRY
BEGIN CATCH
PRINT '=================================================================';
PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error Message'+ CAST(ERROR_STATE() AS NVARCHAR);
PRINT '=================================================================';
END CATCH
END;

GO

EXEC bronze.load_bronze;
