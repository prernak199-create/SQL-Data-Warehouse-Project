-
--checking for Standardisation and consistency 
select distinct gender from gold.dim_customers;
---------------------------------------------------------------------------------------------------------------
select distinct ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr!='n/a' then ci.cst_gndr --CRM is the Master for gender info
ELSE COALESCE(ca.gen,'n/a')
END AS new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid
ORDER BY
1,2;
-------------------------------------------------------------------------------------------------------------------
--check for unique product key in dimention product
--Before creating 'gold.dim_product' and any other changes in column name or sequence
--in the same query so it is written here
--OLD QUERY before creating views 'gold.dim_product'
select prd_key,COUNT(*)FROM (
  select
  pn.prd_id,
  pn.cat_id,
  pn.prd_key,
  pn.prd_nm,
  pn.prd_cost,
  pn.prd_line,
  pn.prd_start_dt,
  pc.subcat,
  pc.maintenance
  from silver.crm-prd_info pn
  LEFT JOIN silver.erp_px_cat_g1v2 pc
  ON pn.cat_id = pc.id
  where prd_end_dt is null ---Filter out all historical data
  )t GROUP BY prd_key
HAVING COUNT(*) > 1;
----------------------------------------------------------------------------------------------------------------------------------------

  




