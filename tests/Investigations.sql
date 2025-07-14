
--=================== Investigate for silver.crm_cust_info table =======================================

--check for duplicates cst_id column
--Expectations : None

select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1;

--check for extra whitespaces on both the firstname and lastname columns
--Expectation : if found remove them
select cst_firstname, cst_lastname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname) or cst_lastname  != TRIM(cst_lastname)


--check for data entry anomalities and standardize them
select distinct cst_marital_status
from bronze.crm_cust_info;


--=================== Investigate for silver.crm_prd_info table =======================================

select prd_id,
	count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1;


select sls_ord_num,
	count(*)
from bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1;

select *
from bronze.crm_sales_details
where sls_ord_num = 'SO55367'


select 
	*
from bronze.crm_sales_details


select distinct maintenance
from bronze.erp_cat_g1v2

select *
from bronze.erp_cust_az12
where bdate > getdate()


select distinct gen
from bronze.erp_cust_az12


select distinct cntry
from bronze.erp_loc_a101

