exec silver.load_silver_layer;

create or alter procedure silver.load_silver_layer as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
begin try
print '========================================================================================================================================'
print '================================================== Load Silver Layer ==================================================================='
print '========================================================================================================================================'

set @batch_start_time = getdate();

print '========================================================================================================================================'
print '================================================== Load CRM Layer ==================================================================='
print '========================================================================================================================================'
set @start_time = getdate()
	print '<<<<<  Truncate table silver.crm_cust_info'

	Truncate table silver.crm_cust_info;
	insert into silver.crm_cust_info(
		cst_id,
		cst_key ,
		cst_firstname ,
		cst_lastname,
		cst_marital_status,
		cst_gndr ,
		cst_create_date 
	)

	select cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			case
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				else 'n/a'
				end cst_marital_status,
			case
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				else 'n/a'
				end cst_gndr,
			cst_create_date
	from
		(select *,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_first
		from bronze.crm_cust_info) sub
	where flag_first =1 and  cst_id is not null;

set @end_time = getdate();

print 'Load Duration for silver.crm_cust_info table' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'

	print '<<<<<  Truncate table silver.crm_prd_info'
set @start_time = getdate()

	truncate table silver.crm_prd_info
	insert into silver.crm_prd_info(
		prd_id ,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost ,
		prd_line ,
		prd_start_dt,
		prd_end_dt 
	)
	select 
		prd_id,
		replace(left(prd_key,5),'-','_')  as cat_id,
		SUBSTRING(prd_key,7, len(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		case
			when UPPER(prd_line) = 'S' then 'Sport'
			when UPPER(prd_line) = 'R' then 'Road'
			when UPPER(prd_line) = 'M' then 'Mountain'
			when UPPER(prd_line) = 'T' then 'Touring'
			else 'Others'
			end prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) over (partition by prd_nm order by prd_start_dt) as prd_end_date
	from
		bronze.crm_prd_info;


set @end_time = getdate()

print 'Load Duration for silver.crm_prd_info' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'

	print '<<<<<  Truncate table silver.crm_sales_details'
	set @start_time = getdate()

	truncate table silver.crm_sales_details
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales,
		sls_quantity ,
		sls_price 
	)
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null
			else cast(CAST(sls_order_dt as varchar) as date) 
			end sls_order_dt,
		case 
			when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then null
			else cast(CAST(sls_ship_dt as varchar) as date) 
			end sls_ship_dt,
		case 
			when sls_due_dt = 0 or LEN(sls_due_dt) != 8 then null
			else cast(CAST(sls_due_dt as varchar) as date) 
			end sls_due_dt,
		case 
			when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * sls_price then sls_quantity * ABS(sls_price)
			else sls_sales
			end sls_sales,
		case 
			when sls_quantity is null or sls_quantity <= 0 then sls_sales/ nullif(sls_price,0)
			else sls_quantity 
			end sls_quantity,
		case 
			when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity,0)
			else sls_price 
			end sls_price
	from 
		bronze.crm_sales_details
set @end_time = getdate()

print 'Load Duration for silver.crm_sales_details' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'


print '========================================================================================================================================'
print '================================================== Load ERP Layer ==================================================================='
print '========================================================================================================================================'


	print '<<<<<  Truncate table silver.erp_cat_g1v2'
set @start_time = getdate()
	truncate table silver.erp_cat_g1v2
	insert into silver.erp_cat_g1v2(
		id ,
		cat ,
		subcat ,
		maintenance
	)

	select
		id,
		cat,
		subcat,
		maintenance
	from 
		bronze.erp_cat_g1v2;

set @end_time = getdate()

print 'Load Duration for silver.erp_cat_g1v2' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'


	print '<<<<<  Truncate table silver.erp_cust_az12'
set @start_time = getdate()
	truncate table silver.erp_cust_az12;
	insert into silver.erp_cust_az12(
		cid ,
		bdate ,
		gen 
	)
	select 
		SUBSTRING(cid,4, len(cid)) as cid,
		case 
			when bdate > GETDATE() then null
			else bdate
			end bdate,
		case 
			when upper(trim(gen)) = 'M' then 'Male'
			when upper(trim(gen)) = 'F' then 'Female'
			when gen is null or gen = ' ' then 'n/a'
			else gen
			end gen
	from 
		bronze.erp_cust_az12;
set @end_time = getdate()

print 'Load Duration for silver.erp_cust_az12' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'

	print '<<<<<  Truncate table silver.erp_loc_a101'
set @start_time = getdate()
	truncate table silver.erp_loc_a101;
	insert into silver.erp_loc_a101(
		cid ,
		cntry 
	)

	select 
		REPLACE(cid, '-','') as cid,
		case
			when cntry ='DE' then 'Germany'
			when cntry in ('US', 'USA') then 'United States'
			when cntry is null or cntry = ' ' then 'n/a'
			else cntry
			end cntry
	from
		bronze.erp_loc_a101;
set @end_time = getdate()

print 'Load Duration for silver.erp_cust_az12' + cast(datediff(second, @start_time,@end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'


set @batch_end_time = getdate()

print 'Load Duration for Silver Batch' + cast(datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + ' Seconds'
print ' <<<<<<<============================================================================================'
end try

begin catch
	print ' ======================================================================================================= '
	print'	=======================================Error During Loading Silver layer================================'
	print ' Error Message ' + error_message()
	print ' Error Message ' + cast(error_number() as nvarchar)
	print ' Error Message ' + cast(error_state() as nvarchar)
	print ' ======================================================================================================= '
end catch
end;

