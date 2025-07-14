if OBJECT_ID('gold.dim_customer','U') is not null
	drop view gold.dim_customer;
	go

create or alter view gold.dim_customer as
select 
		row_number() over (order by cst_id) as customer_number,
		cst_id as customer_id,
		cst_key as customer_key,
		cst_firstname as first_name,
		cst_lastname as last_name,
		case
			when ci.cst_gndr = 'n/a' then ca.gen
			else ci.cst_gndr
			end gender,
		ca.bdate as birth_date,
		lo.cntry as country
		
from 
	silver.crm_cust_info ci
left join
	silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join
	silver.erp_loc_a101 lo
on ci.cst_key = lo.cid;

go


if OBJECT_ID('gold.dim_products', 'V') is not null
	drop view gold.dim_products;
go
create or alter view  gold.dim_products as
select
	row_number() over (order by prd_id, prd_start_dt) as product_number,
	prd_id as product_id,
	prd_key as product_key,
	prd_nm as product_name,
	prd_line as product_line,
	prd_cost as cost,
	cg.cat category,
	cg.subcat  as sub_category,
	prd_start_dt as product_start_date,
	cg.maintenance
from
	silver.crm_prd_info pi
left join 
	silver.erp_cat_g1v2 cg
on pi.cat_id = cg.id
where prd_end_dt is null;

go


if OBJECT_ID('gold.fact_sales', 'V') is not null
	drop view gold.fact_sales
go
create or alter view gold.fact_sales as 
select
	sls_ord_num as order_id,
	dc.customer_number,
	dp.product_number,
	sls_order_dt as order_date,
	sls_ship_dt as shipping_date,
	sls_due_dt as due_date,
	sls_sales as sales,
	sls_quantity as quantity,
	sls_price as price
from silver.crm_sales_details sd
left join gold.dim_customer dc
on sd.sls_cust_id = dc.customer_id 
left join gold.dim_products dp
on sd.sls_prd_key = dp.product_key ;


