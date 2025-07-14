
 --=================== Test for silver.crm_cust_info table =======================================

--check for duplicates cst_id column
--Expectations : None

select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1;

--check for extra whitespaces on both the firstname and lastname columns
--Expectation : if found remove them
select cst_firstname, cst_lastname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname) or
cst_lastname  != TRIM(cst_lastname)


--check for data entry anomalities and standardize them
select distinct cst_marital_status
from silver.crm_cust_info;


select distinct cst_gndr
from silver.crm_cust_info;