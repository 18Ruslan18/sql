use testdb
truncate table movegood
drop table movegood
create table movegood(
id int not null,
src_warehouse_id int,
dst_warehouse_id int,
amount float,
status varchar(15),
status_name varchar(15),
created_date datetime,
line_id int,
item_id int,
quantity float,
price float,
amount2 float
);

alter procedure OpenXmlMovegood @xmlWay nvarchar(300) 
as
begin
--перменные для замера времени выполнения процедуры
declare @timeStart datetime 
set @timeStart=getdate()
declare @timeEnd datetime 
begin try


  declare @xml XML
declare @find nvarchar(50) = 'encoding="UTF-8"'
declare @replacement nvarchar(50) = 'encoding="Windows-1251"'
  declare @sql nvarchar(max) = N'set @xml= replace((select BulkColumn from openrowset(bulk  '''+ @xmlWay+'''
,single_clob)  x),'''+@find+''','''+@replacement+''');';
exec sp_executesql @sql, N'@xml xml OUTPUT', @xml OUTPUT;
--переменые для подсчета кол-ва добавленных строк
declare @count_new int
declare @count_old int
declare @count_add int
set @count_old = (select count(*) from Movegood) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/movegoods/movegood/items/item/line_id',2) 
with (
 id int '../../../id',
src_warehouse_id int '../../../src_warehouse_id',
dst_warehouse_id int '../../../dst_warehouse_id',
amount float '../../../amount',
status varchar(15) '../../../status',
status_name varchar(15) '../../../status_name',
created_date datetime '../../../created_date ',
line_id int '.',
item_id int '../item_id',
quantity float '../quantity',
price float '../price',
amount2 float '../amount')
declare @maxdate datetime = (select max(created_date) from #temp)
declare @mindate datetime = (select min(created_date) from #temp)
delete from movegood where created_date between @mindate and @maxdate
insert into Movegood (id ,
src_warehouse_id,
dst_warehouse_id ,
amount ,
status ,
status_name ,
created_date ,
line_id ,
item_id ,
quantity ,
price ,
amount2 )
select id ,
src_warehouse_id,
dst_warehouse_id ,
amount ,
status ,
status_name ,
created_date ,
line_id ,
item_id ,
quantity ,
price ,
amount2 from #temp
execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Movegood) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlMovegood','Movegood',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlMovegood','Movegood',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\movegood_new.xml' ,single_blob) as x
exec OpenXmlMovegood 'C:\Users\DELL\Desktop\virtualpos\movegood.xml'

select * from movegood

create clustered index CIndexMovegood
on Movegood(id) on PartitionSchemeMovegood(created_date)

create partition function PartitionFunckMovegood (datetime) as range 
for values ('1.1.2019')

create partition scheme PartitionSchemeMovegood  as partition PartitionFunckMovegood
to ([Быстрые таблицы],[Быстрые таблицы])

create index N1IndexMovegood on Movegood(src_warehouse_id)

create index N2IndexMovegood on Movegood(dst_warehouse_id)

create index N3IndexMovegood on Movegood(line_id)

create index N4IndexMovegood on Movegood(item_id)