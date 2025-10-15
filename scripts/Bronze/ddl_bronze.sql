create or alter procedure bronze.load_bronze as
begin
	declare @start_time Datetime,@End_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	begin try 
		set @batch_start_time = GETDATE();
		print '============================================';
		print 'Loading the Bronze Layer';
		print '============================================';

		print '---------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.crm_cust_info ';
		truncate table bronze.crm_cust_info;
		print '>> Inserting table: bronze.crm_cust_info ';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\nithi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		with 
		( 
			firstrow = 2 ,
			fieldterminator = ',',
			tablock 
		);
		set @End_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>> Inserting table: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\nithi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
		with 
		( 
			firstrow = 2 ,
			fieldterminator = ',',
			tablock 
		);
		set @End_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>> Inserting table: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\nithi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
		with 
		( 
			firstrow = 2 ,
			fieldterminator = ',',
			tablock 
		);
		set @End_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';

		print '---------------------------------------------';
		print 'Loading ERP Tables';
		print '---------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>> Inserrting table: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\nithi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		with 
		( 
			firstrow = 2 ,
			fieldterminator = ',',
			tablock 
		);
		set @End_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>> Inserting table: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\nithi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		with 
		( 
			firstrow = 2 ,
			fieldterminator = ',',
			tablock 
		);
		set @End_time = GETDATE();
		print '>> Load Duration:' + cast(datediff( second, @start_time, @End_time ) as nvarchar) + ' seconds';
		print '------------------------------------------------------------';


		set @start_time = GETDATE();
		print '>> Truncating table: bronze.erp_PX_CAT_G1V2';
		truncate table bronze.erp_PX_CAT_G1V2;
		print '>> Inserting table: bronze.erp_PX_CAT_G1V2';
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\nithi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		with 
		( 
			firstrow = 2 ,
			fieldterminator = ',',
			tablock 
		);
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
