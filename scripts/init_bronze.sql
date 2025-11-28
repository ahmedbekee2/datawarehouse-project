create or alter procedure bronze.load_bronze as 
Begin

Declare @starttime datetime ,  @endtime datetime ;
begin try 

PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		PRINT '>> Truncating Table: bronze.crm_cust_info';
Truncate table bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		set @starttime = GETDATE(); 

bulk insert bronze.crm_cust_info 

from 'D:\ITI\DWH\task datawarehouse baraa\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'

with ( 
 firstrow  =2 ,
 fieldterminator = ',' , 
 tablock 
);
--################################################   1
set @endtime = GETDATE() ; 
		PRINT '>> Truncating Table: bronze.crm_pre_info';

Print '>> Duration' +cast(datediff(second , @starttime , @endtime ) as nvarchar) +'seconds' ;
Truncate table bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_pre_info';
				set @starttime = GETDATE(); 

bulk insert bronze.crm_prd_info 

from 'D:\ITI\DWH\task datawarehouse baraa\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'

with ( 
 firstrow  =2 ,
 fieldterminator = ',' , 
 tablock 
);
--#################################################   2 
set @endtime = GETDATE() ; 


	PRINT '>> Truncating Table: bronze.crm_sales_details';
	Print '>> Duration' +cast(datediff(second , @starttime , @endtime ) as nvarchar) +'seconds' ;

Truncate table bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		set @starttime =getdate();
bulk insert bronze.crm_sales_details 

from 'D:\ITI\DWH\task datawarehouse baraa\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'

with ( 
 firstrow  =2 ,
 fieldterminator = ',' , 
 tablock 
);
--#################################################### 3 
      set @endtime = getdate()

	PRINT '>> Truncating Table: bronze.erp_cust_az12';

Print '>> Duration' +cast(datediff(second , @starttime , @endtime ) as nvarchar) +'seconds' ;
Truncate table bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
				set @starttime =getdate()

bulk insert bronze.erp_cust_az12 

from 'D:\ITI\DWH\task datawarehouse baraa\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'

with ( 
 firstrow  =2 ,
 fieldterminator = ',' , 
 tablock 
);
--#####################################################33  4 

		set @endtime =getdate();


	PRINT '>> Truncating Table: bronze.erp_loc_a101';

Print '>> Duration' +cast(datediff(second , @starttime , @endtime ) as nvarchar) +'seconds' ;

Truncate table bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
set @starttime = getdate() ; 
bulk insert bronze.erp_loc_a101 

from 'D:\ITI\DWH\task datawarehouse baraa\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'

with ( 
 firstrow  =2 ,
 fieldterminator = ',' , 
 tablock 
);
set @endtime = getdate();
-- ####################################################### 5


	PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		print '>>>duration' + cast(datediff (second ,@starttime , @endtime ) as varchar) + 'seconds '  ; 


Truncate table bronze.erp_px_cat_g1v2;	
	PRINT '>> Inserting Data Into: erp_px_cat_g1v2.erp_loc_a101';
	set @starttime = getdate() ; 

bulk insert bronze.erp_px_cat_g1v2 

from 'D:\ITI\DWH\task datawarehouse baraa\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'

with ( 
 firstrow  =2 ,
 fieldterminator = ',' , 
 tablock 
);
set @endtime = getdate();
Print '>> Duration' +cast(datediff(second , @starttime , @endtime ) as nvarchar) +'seconds' ;


 end try 
 begin catch 
 PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
 end catch 

end ;

exec bronze.load_bronze
