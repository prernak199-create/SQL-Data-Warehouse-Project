/*Create Gold View */
---Dimension Customer
CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) as cutomer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status,
CASE WHEN ci.cst_gndr!='n/a' then ci.cst_gndr --CRM is the Master for gender info
ELSE COALESCE(ca.gen,'n/a')
END AS gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN
silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN
silver.erp_loc_a101 la
ON ci.cst_key=la.cid;
--------------------------
select* from gold.dim_customers;
-----------------------------------------------------------------------------------------------------------------------

CREATE VIEW gold.dim_product AS
select 
ROW_NUMBER()OVER(ORDER BY pn.prd_start_dt,pn.prd_key)as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt is null-- Filter out all historical data
---
SELECT* FROM gold.dim_product;
--------------------------------------------------------------------------------------------------------------------------

create view gold.fact_sales as
select
sd.sls_ord_num,
pr.product_key,
cu.cutomer_key as customer_key,
--------cutomer_key insted of customer
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id;
-----
select* from gold.fact_sales;
------------------------------------------------------------------------------------------------------------------------------
