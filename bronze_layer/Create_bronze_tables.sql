/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

-- =============================================================================
-- Creat bronze.crm_cust_info
-- =============================================================================


if OBJECT_ID('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;
go

create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_marital_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date,
	
);
go


-- =============================================================================
-- Creat bronze.crm_prd_info
-- =============================================================================


if OBJECT_ID('bronze.crm_prd_info','U') is not null
	drop table bronze.crm_prd_info;
go


create table bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line varchar(50),
	prd_start_dt date,
	prd_end_dt date
);
go

-- =============================================================================
-- Creat bronze.crm_sales_details
-- =============================================================================


if OBJECT_ID('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details;
go

create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);
go

-- =============================================================================
-- Creat bronze.erp_cust_az12
-- =============================================================================


if object_id('bronze.erp_cust_az12', 'U') is not null
	drop table bronze.erp_cust_az12;
go

create table bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen varchar(50)
);
go



-- =============================================================================
-- Creat bronze.erp_loc_a101
-- =============================================================================


if object_id('bronze.erp_loc_a101', 'U') is  not null
	drop table bronze.erp_loc_a101;
go

create table bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry varchar(50),
);
go


-- =============================================================================
-- Creat silver. bronze.erp_cat_g1v2
-- =============================================================================


if object_id('bronze.erp_cat_g1v2','U') is not null
	drop table bronze.erp_cat_g1v2;
go
create table bronze.erp_cat_g1v2(
	id varchar(50),
	cat varchar(50),
	subcat varchar(50),
	maintenance varchar(50)
);
go




