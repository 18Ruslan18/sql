use testdb
drop table Income
create table Income (
id int not null ,
warehouse_id tinyint,
supplier_id tinyint,
docdate datetime,
status varchar(30),
amount float,
comment varchar(100),
created_date datetime,
item_id int,
quantity float,
price float

);

alter procedure OpenXmlIncome @xmlWay nvarchar(300)
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
set @count_old = (select count(*) from Income) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/inflows/inflow/items/item/item_id',2) 
with (
  id int '../../../id' ,
warehouse_id tinyint '../../../warehouse_id' ,
supplier_id tinyint '../../../supplier_id' ,
docdate datetime '../../../docdate' ,
status varchar(30) '../../../status' ,
amount float '../../../amount' ,
comment varchar(100) '../../../comment ' ,
created_date datetime '../../../created_date' ,
item_id int '.' ,
quantity float '../quantity',
price float '../price')
declare @maxdate datetime = (select max(docdate) from #temp)
declare @mindate datetime = (select min(docdate) from #temp)
delete from Income where docdate between @mindate and @maxdate
insert into Income ( id ,
warehouse_id ,
supplier_id ,
docdate ,
status ,
amount ,
comment ,
created_date ,
item_id ,
quantity ,
price )
select id ,
warehouse_id ,
supplier_id ,
docdate ,
status ,
amount ,
comment ,
created_date ,
item_id ,
quantity ,
price from #temp
execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Income) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlIncome','Income',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlIncome','Income',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\income_new.xml' ,single_blob) as x
exec OpenXmlIncome 'C:\Users\DELL\Desktop\virtualpos\income.xml'

select * from Income

create clustered index CIndexIncome
on Income(id) on PartitionSchemeIncome(docdate)

create partition function PartitionFunckIncome (datetime) as range 
for values ('1.1.2019')

create partition scheme PartitionSchemeIncome  as partition PartitionFunckIncome
to ([Быстрые таблицы],[Быстрорастущие таблицы])

create index N1IndexIncome on Income(warehouse_id)

create index N2IndexIncome on Income(supplier_id)

create index N3IndexIncome on Income(item_id)


