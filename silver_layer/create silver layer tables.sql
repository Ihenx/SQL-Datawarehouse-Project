if OBJECT_ID('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info;
go

create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_marital_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date,
	
);
go




if OBJECT_ID('silver.crm_prd_info','U') is not null
	drop table silver.crm_prd_info;
go


create table silver.crm_prd_info(
	prd_id int,
	cat_id varchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line varchar(50),
	prd_start_dt date,
	prd_end_dt date
);
go



if OBJECT_ID('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details;
go

create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int
);
go



if object_id('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12;
go

create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen varchar(50)
);
go





if object_id('silver.erp_loc_a101', 'U') is  not null
	drop table silver.erp_loc_a101;
go

create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry varchar(50),
);
go




if object_id('silver.erp_cat_g1v2','U') is not null
	drop table silver.erp_cat_g1v2;
go
create table silver.erp_cat_g1v2(
	id varchar(50),
	cat varchar(50),
	subcat varchar(50),
	maintenance varchar(50)
);
go




