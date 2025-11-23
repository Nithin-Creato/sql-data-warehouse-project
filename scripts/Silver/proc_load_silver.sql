exec silver.load_silver

create or alter procedure silver.load_silver as
begin
	declare @start_time Datetime,@End_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	begin try 
		set @batch_start_time = GETDATE();
		print '============================================';
		print 'Loading the Silver Layer';
		print '============================================';

		print '---------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------';
		set @start_time = GETDATE();
	print '>> Truncating the table: silver.crm_cust_info';
	truncate table silver.crm_cust_info
	print '>> Inserting data into: silver.crm_cust_info';
	Insert into silver.crm_cust_info 
	(  
		cst_id, 
		cst_key, 
		cst_firstname,
		cst_lastname,
		cst_marital_status, 
		cst_gndr, 
		cst_create_date 
	)
	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case 
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		when upper(trim(cst_gndr)) = 'F' then 'Female'
		else 'unknown'
	end cst_gndr,
	case 
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
		when upper(trim(cst_marital_status)) = 'S' then 'Single'
		else 'unknown'
	end cst_marital_status,
	cst_create_date
	from (

	select 
	*,
	ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
	from 
	bronze.crm_cust_info where cst_id is not null)t where flag_last = 1
	set @End_time = GETDATE();
	print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
	print '------------------------------------------------------------';


	set @start_time = GETDATE();
	print '>> Truncating the table: silver.crm_prd_info';
	truncate table silver.crm_prd_info
	print '>> Inserting data into: silver.crm_prd_info';
	insert into silver.crm_prd_info (
		prd_id,       
		cat_id,   
		prd_key,  
		prd_nm,      
		prd_cost,  
		prd_line,    
		prd_start_dt,
		prd_end_dt
	)

	select 
	prd_id,
	--Derived New Columns.
	REPLACE(substring(prd_key,1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
	prd_nm,
	--Handling missing information.
	isnull(prd_cost, 0) as prd_cost,
	case 
		when upper(trim(prd_line)) = 'M' then 'Mountain'
		when upper(trim(prd_line)) = 'R' then 'Road'
		when upper(trim(prd_line)) = 'S' then 'Other Sales'
		when upper(trim(prd_line)) = 'T' then 'Touring'
		else 'n/a' 
	end prd_line,
	--converting one datatype to another one.
	cast(prd_start_dt as date ) as prd_start_dt,
	--data Enrichment
	cast(lead(prd_start_dt) over( partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt
	from
	bronze.crm_prd_info
	set @End_time = GETDATE();
	print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
	print '------------------------------------------------------------';

	set @start_time = GETDATE();
	print '>> Truncating the table: silver.crm_sales_details';
	truncate table silver.crm_sales_details
	print '>> Inserting data into: silver.crm_sales_details';
	insert into silver.crm_sales_details
	(  
		sls_ord_num, 
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	 )
	select
		sls_ord_num,  
		sls_prd_key,  
		sls_cust_id,  
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date) 
		end sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date) 
		end sls_ship_dt,
		 case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			else cast(cast(sls_due_dt as varchar) as date) 
		end sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	from bronze.crm_sales_details
	set @End_time = GETDATE();
	print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
	print '------------------------------------------------------------';


	print '---------------------------------------------';
	print 'Loading ERP Tables';
	print '---------------------------------------------';
	set @start_time = GETDATE();
	print '>> Truncating the table: silver.erp_cust_az12';
	truncate table silver.erp_cust_az12
	print '>> Inserting data into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12 ( cid, bdate, gen)
	select 
	case when 
		cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
		else cid
		end cid,
		case 
			when  BDATE > GETDATE() then null else BDATE
			end as BDATE,
		case 
			when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
			when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
			else 'n/a'
			end gen
	from bronze.erp_cust_az12
	set @End_time = GETDATE();


	set @start_time = GETDATE();
	print '>> Truncating the table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101
	print '>> Inserting data into: silver.erp_loc_a101';
	insert into silver.erp_loc_a101( cid, cntry)
	select 
	REPLACE(cid, '-', '') cid,
	case 
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or cntry is null then  'n/a'
		else TRIM(cntry) 
	end cntry
	from bronze.erp_loc_a101
	set @End_time = GETDATE();
	print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
	print '------------------------------------------------------------';

	set @start_time = GETDATE();
	print '>> Truncating the table: silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2
	print '>> Inserting data into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2 (id,
	cat,
	subcat,
	maintenance )
	select 
	id,
	cat,
	subcat,
	maintenance 
	from 
	bronze.erp_PX_CAT_G1V2

		set @End_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';

		set @batch_end_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @batch_start_time, @batch_end_time  ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';

	end try 
	begin catch 
		print '============================================';
		print 'Error occured during loading bronze layer';
		print 'Error Message' + Error_Message();
		print 'Error Message' + cast(Error_number() as nvarchar);
		print 'Error Message' + cast(Error_state() as nvarchar);
		print '============================================';
	end catch
end 
