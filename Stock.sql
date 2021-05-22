use testdb
drop table Stock
truncate Stock
create table Stock (
MyPrimId int identity(1,1),
id int,
name varchar(60),
id3 int,
quantity float,
cogs float,
insertDate date,
primary key(MyPrimId)
);

alter procedure OpenXmlStock @xmlWay nvarchar(300) 
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
set @count_old = (select count(*) from Stock) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/warehouses/warehouse/items/item/id' ,2) 
with (
  id int '../../../id'  ,
name varchar(60) '../../../name' ,
id3 int '.'  ,
quantity float '../quantity'    ,
cogs float  '../cogs')
declare @checkDate date = convert(date,getdate())
delete from Stock where insertDate=@checkDate
insert into Stock (id ,
name ,
id3 ,
quantity ,
cogs )
select id ,
name ,
id3 ,
quantity ,
cogs from #temp
execute sp_xml_removedocument @xml_output
update Stock set insertDate = convert(date,getdate())
set @count_new = (select count(*) from Stock) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlStock','Stock',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlStock','Stock',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\stock_new.xml' ,single_blob) as x
exec OpenXmlStock 'C:\Users\DELL\Desktop\virtualpos\stock.xml'

select * from Stock 
truncate table Stock
create clustered index CIndexStock
on Stock(id3) on PartitionSchemeStock(id)

create index N1IndexStock on Stock(name)

create partition function PartitionFunckStock (int) as range 
for values (10,20)

create partition scheme PartitionSchemeStock  as partition PartitionFunckStock
to ([Быстрорастущие таблицы],[Быстрорастущие таблицы],[Быстрорастущие таблицы])


select * from goods



