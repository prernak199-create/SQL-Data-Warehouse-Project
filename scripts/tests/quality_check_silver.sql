/*
***********************************************************************************************************************
Quality Check
***********************************************************************************************************************
Script Purpose:
This script perform various quality check for data consistency,accurency and standardisationin the 'silver' schema.
this include check for:
-null or duplicate Primary Key
-unwanted spaces in string
-Data standardisation
-Invalid Date ranges and orders
-data consistency betwen related fields.
*/
-------------------------------------------------------------------------------------------------------------------------
--CHECK QUALITY OF SILVER
--Re-run the quality check queries from the bronze layer to varify the quality of the data in silver layer.
--Check for null or duplicates in primary key
-------------------------------------------------------------------------------------------------------------------------
SELECT cst_id,COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*)>1 OR cst_id IS NULL
--Check for spaces in string
--Expectation:No Result
SELECT cst_firstname 
from silver.crm_cust_info
where cst_firstname !=TRIM(cst_firstname)
--if the original value is not equalto the same value after triming it means there are spaces
--CHECK FOR UNWANTED SPACES IN LASTNAME
--EXPECTSTION=No Result
SELECT cst_lastname
FROM silver.crm_cust_info
where cst_lastname !=TRIM(cst_lastname)
--FOR CHECKING SPACES IN GENDER
SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr !=TRIM(cst_gndr)
--DATA Standardization OR ABBREVIATION and consistency check
SELECT  DISTINCT cst_gndr
FROM silver.crm_cust_info
--FOR MARITALSTATUS
SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info;
----------------------------------------------------------------------------------------------------------------------------------

--QUALITY CHECK
--Check for null or duplicate Primary Key in silver.crm_prd_info table
--Expectation:No Result
SELECT
prd_id,COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL
--CHECK FOR UNWANTED SPACES
SELECT prd_nm 
from silver.crm_prd_info
where prd_nm !=TRIM(prd_nm)
--check for nulls or negative numbers
--expectation:No result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL
--DATA Standardization OR ABBREVIATION and consistency check
SELECT  DISTINCT prd_line
FROM silver.crm_prd_info
--Check for Invalid Date Orders
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt< prd_start_dt;
select * from silver.crm_prd_info
----------------------------------------------------------------------------------------------------------------------
--Check for Invalid Date Orders
select * from silver.crm_sales_details
where sls_order_dt> sls_ship_dt OR sls_order_dt> sls_due_dt
--CHECK DATA CONSISTENCY BETWEEN: Sales,Quantity ,Price
-->> Sales = Quantity * Price
-->> Values must not be -ve,null,or zero
Select  DISTINCT sls_sales,
sls_quantity,
sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL 
or sls_quantity is null
or sls_price  is null
OR sls_sales <=0
or sls_quantity<=0
or sls_price<=0
ORDER BY sls_sales,sls_quantity,sls_price;
select * from silver.crm_sales_details
------------------------------------------------------------------------------------------------------------------------------

 --DATA QUALITY CHECK OF silver.erp_cust_az12
 --Identify Out of Range Dates Very old Customer
SELECT bdate from silver.erp_cust_az12
WHERE bdate<'1924-01-01' OR 
 bdate > GETDATE()
 --Data STANDARDIZATION
 SELECT DISTINCT gen
 from silver.erp_cust_az12;
 SELECT* FROM silver.erp_cust_az12;
----------------------------------------------------------------------------------------------------------------------------------

--check
SELECT cst_key from silver.crm_cust_info
--check for standardization
select distinct cntry from bronze.erp_loc_a101
order by cntry;
--check in silver
select distinct cntry from silver.erp_loc_a101;
SELECT* FROM silver.erp_loc_a101;
-----------------------------------------------------------------------------------------------------------------------------------

--check Quality
---check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat!=TRIM(cat) or subcat!=TRIM(subcat)
or maintenance!=TRIM(maintenance)
--DATA STANDARDISATION CHECK
SELECT DISTINCT cat from bronze.erp_px_cat_g1v2;
select distinct subcat from bronze.erp_px_cat_g1v2;
SELECT distinct maintenance from bronze.erp_px_cat_g1v2;





SELECT * FROM silver.crm_cust_info
