/****** Скрипт для команды SelectTopNRows из среды SSMS  ******/
use OrganicNeva_Abdulin
SELECT  [dt]
      ,[OperTypeID]
      ,[id_store]
      ,[id_goods]
      ,[id_supplier]
      ,[quantity_in]
      ,[quantity_out]
      ,[quantity]
      ,[Quantity_Rest]
      ,[sale_in]
      ,[sale_out]
      ,[sale]
      ,[sale_Rest]
      ,[Cost_in]
      ,[Cost_out]
      ,[Cost]
      ,[Cost_Rest]
      ,[Price of list]
  FROM [OrganicNeva_Abdulin].[dbo].[lasmart_v_fact_movement] where dt=20180305

  alter procedure dbo.lasmart_fill_slice_on_movement @start_date date, @end_date date
  as
  begin
  declare
  @name varchar(500) = '['+ OBJECT_SCHEMA_NAME(@@PROCID)+'].['+OBJECT_NAME(@@PROCID)+']'
		,@description varchar(500) = 'Срез по таблице lasmart_v_fact_movement'
		,@input_parametrs varchar(500) = '@start_date date, @end_date date'
		,@sql varchar(max)
		,@sql_openquery varchar(max)
  
  begin try
  EXEC oth_fill_SUP_LOG @name = @name,	@state_name = 'start', @sp_id = @@SPID, @description = @description, @input_parametrs  = @input_parametrs
  declare @dt_start int = convert (int,convert(varchar(4),year(@start_date))+convert(varchar(2),@start_date,101)+right(convert(varchar(10),@start_date),2))
  declare @dt_end int = convert (int,convert(varchar(4),year(@end_date))+convert(varchar(2),@end_date,101)+right(convert(varchar(10),@end_date),2))
	select * into #temp from (select left(dt,6) as dt, id_store, id_goods,sum(quantity) as quantity, sum(sale_out) as sale_out,sum(sale) as sale, 
		sum(cost_out) as cost_out, sum(cost) as cost 
		from dbo.lasmart_v_fact_movement 
		where dt between @dt_start and @dt_end
		group by (left(dt,6)), id_store, id_goods ) as x
	merge bi_slise_process as target
	using 
	(select dt,id_store, id_goods, 
		sum(quantity) over (partition by id_store, id_goods order by dt) as slice_quantity,
		sum(sale_out) over (partition by id_store, id_goods order by dt) as sale_out,
		sum(sale) over (partition by id_store, id_goods order by dt) as sale,
		sum(cost_out) over (partition by id_store, id_goods order by dt) as cost_out,
		sum(cost) over (partition by id_store, id_goods order by dt) as cost
		from #temp) as source
		on (target.dt=source.dt and target.id_store=source.id_store and target.id_goods=source.id_goods)
		when matched and ( target.slice_quantity<>source.slice_quantity or target.sale<>source.sale or target.cost<> source.cost)
		then update set target.slice_quantity=source.slice_quantity, target.sale=source.sale,
		target.sale_out =source.sale_out, target.cost=source.cost, target.cost_out=source.cost_out
		when not matched 
		then insert values(source.dt,source.id_store, source.id_goods, source.slice_quantity, source.sale_out,
		source.sale, source.cost_out, source.cost);
    EXEC oth_fill_SUP_LOG  @name = @name, @state_name = 'finish', @sp_id = @@SPID, @description = @description, @input_parametrs = @input_parametrs
  end try
  begin catch
		EXEC oth_fill_SUP_LOG  @name = @name, @state_name = 'error', @sp_id = @@SPID, @description = @description, @input_parametrs = @input_parametrs
  end catch
  end 
  
  exec  dbo.lasmart_fill_slice_on_movement '2018-01-25', '2018-04-30'
 
 drop table bi_slise_process
  create table bi_slise_process(
  dt int,id_store int,id_goods int, slice_quantity int, sale_out money, sale money, cost_out money, cost money
  )
  select * from bi_slise_process order by id_store, id_goods, dt
  
  
  select left(dt,6) as dt, id_store, id_goods,sum(quantity)+
  (select sum(quantity) from dbo.lasmart_v_fact_movement where id_store=a.id_store and id_goods=a.id_goods and dt>a.dt group by (left(dt,6)), id_store, id_goods  )
  , sum(sale_out) as sale_out,sum(sale) as sale, 
	sum(cost_out) as cost_out, sum(cost) as cost 
	from dbo.lasmart_v_fact_movement a
group by (left(dt,6)), id_store, id_goods
	order by left(dt,6)
  
  
  
  
  
  
  
  
  
  convert(char(2), prod_date, 101)
  declare @test date
  set @test = getdate()
    declare @test1 varchar(60)
	set @test1=convert(varchar(4),year(@test))+convert(varchar(2),@test,101)+right(convert(varchar(10),@test),2)
	declare @t int = convert (int,@test1)
	print @t

	select * from oth_SUP_LOG


	CREATE procedure oth_fill_sup_log
	@name varchar(255) = null,		--obj_name
	@state_name varchar(255) = null,	--start, finish, error
	@row_count int = null,	
	@sp_id int = null,
	@description nvarchar(500) = null,
	@input_parametrs nvarchar(500) = null
as
begin

	
	insert into oth_SUP_LOG
	(
		 [date_time]
		,[name]
		,[system_user]
		,[state_name]
		,[row_count]
		,[err_number]
		,[err_severity]
		,[err_state]
		,[err_object]
		,[err_line]
		,[err_message]
		,[sp_id]
		,[duration]
		,[duration_ord]
		,[description]
        ,[input_parametrs]
	)
	select 
		getdate()
		,@name
		,system_user
		,@state_name
		,case 
			when @state_name = 'finish' and @row_count is null then @@rowcount 
			when @state_name = 'finish' and @row_count is not null then @row_count
			when @state_name = 'error' then -1 
			else null 
		end
		,error_number()
		,error_severity()
		,error_state()
		,error_procedure()
		,error_line()
		,error_message()
		,@sp_id
		,case 
			when @state_name = 'start' then null
			else 				 
				 cast(cast((DATEDIFF(ss,(select max(date_time) 
										from oth_SUP_LOG
										where state_name = 'start' 
											and name = @name 
											and sp_id = @sp_id),getdate()))/3600 as int) as varchar(3)) 
				  +':'+ right('0'+ cast(cast(((DATEDIFF(ss,(select max(date_time) 
															from oth_SUP_LOG
															where state_name = 'start' 
																and name = @name 
																and sp_id = @sp_id),getdate()))%3600)/60 as int) as varchar(2)),2) 
				  +':'+ right('0'+ cast(((DATEDIFF(ss,(select max(date_time) 
														from oth_SUP_LOG
														where state_name = 'start' 
															and name = @name 
															and sp_id = @sp_id),getdate()))%3600)%60 as varchar(2)),2) +' (hh:mm:ss)'
		end
		,case 
			when @state_name = 'start' then null
			else 				 
				 DATEDIFF(ss,(select max(date_time) 
								from oth_SUP_LOG
								where state_name = 'start' 
									and name = @name 
									and sp_id = @sp_id),getdate())
		end
		,@description
		,@input_parametrs

	WAITFOR DELAY '00:00:00.100'
end
select * from oth_SUP_LOG
create table oth_SUP_LOG
	(
		 [date_time] date
		,[name] varchar(50)
		,[system_user] varchar(50)
		,[state_name] varchar(50)
		,[row_count] int
		,[err_number] varchar(50)
		,[err_severity]varchar(50)
		,[err_state]varchar(50)
		,[err_object]varchar(50)
		,[err_line]varchar(50)
		,[err_message]varchar(50)
		,[sp_id]varchar(50)
		,[duration]varchar(50)
		,[duration_ord]varchar(50)
		,[description]varchar(50)
        ,[input_parametrs]varchar(50)
	)

	select convert(date,convert(varchar(10),dt)), sum(quantity), id_store,id_goods from dbo.lasmart_v_fact_movement group by month(convert(date,convert(varchar(10),dt))), id_store, id_goods order by id_store 
	select dt from dbo.lasmart_v_fact_movement order by dt
	select sum(quantity), id_store,id_goods from dbo.lasmart_v_fact_movement group by id_store, id_goods order by id_store 

	
	select convert(date,convert(varchar(10),dt)), quantity into test from dbo.lasmart_v_fact_movement 
	CREATE TABLE test
(date1 date,
sumn int)
 
INSERT INTO #test

 
SELECT * FROM #test
	
	select convert(date,convert(varchar(10),dt)), sum(quantity) from dbo.lasmart_v_fact_movement group by month(convert(date,convert(varchar(10),dt))) 
	---  dt int,id_store int,id_goods int, slice int, sale_out float, sale float, cost_out float, cost float


	drop table bi_slise_process
  create table bi_slise_process(
  dt int,id_store int,id_goods int, slice int, sale_out float, sale float, cost_out float, cost float
  )
  select * from bi_slise_process

WITH temp3 AS
(
   select left(dt,6) as dt, id_store, id_goods,sum(quantity) as quantity, sum(sale_out) as sale_out,sum(sale) as sale, 
	sum(cost_out) as cost_out, sum(cost) as cost 
	from dbo.lasmart_v_fact_movement 
group by (left(dt,6)), id_store, id_goods    
)
insert into bi_slise_process
select dt,id_store, id_goods, 
sum(quantity) over (partition by id_store, id_goods order by dt) as slice_quantity,
sum(sale_out) over (partition by id_store, id_goods order by dt) as sale_out,
sum(sale) over (partition by id_store, id_goods order by dt) as sale,
sum(cost_out) over (partition by id_store, id_goods order by dt) as cost_out,
sum(cost) over (partition by id_store, id_goods order by dt) as cost
from temp3