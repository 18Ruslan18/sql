alter procedure dbo._p_lasmart_fill_repotrt_remainder( @dt int, @id_store int)
--returns @out_report_remainder table (Store varchar(20), goodgroups varchar(35), goods varchar(35), 
--									remainder_quantity int, remainder_money money )
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
  --insert into @out_report_remainder (Store, goodgroups, goods, remainder_quantity, remainder_money)
  select lasmart_dim_stores.name, lasmart_dim_goodgroups.name,  
  lasmart_dim_goods.name, slice_quantity, sale_out-cost_out as remainder_money
  from bi_slise_process join
  lasmart_dim_stores on bi_slise_process.id_store=lasmart_dim_stores.store_id join
  lasmart_dim_goods on bi_slise_process.id_goods=lasmart_dim_goods.good_id join
  lasmart_dim_goodgroups on lasmart_dim_goods.group_id =  lasmart_dim_goodgroups.goodgroup_id
  where dt=@dt and bi_slise_process.id_store=@id_store
  --return
   EXEC oth_fill_SUP_LOG  @name = @name, @state_name = 'finish', @sp_id = @@SPID, @description = @description, @input_parametrs = @input_parametrs
  end try
  begin catch
		EXEC oth_fill_SUP_LOG  @name = @name, @state_name = 'error', @sp_id = @@SPID, @description = @description, @input_parametrs = @input_parametrs
  end catch
  end 

  exec dbo._p_lasmart_fill_repotrt_remainder 201802, 1