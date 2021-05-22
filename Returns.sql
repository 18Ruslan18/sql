use testdb
drop table Returns
create table Returns(
MyPrimId int identity(1,1),
id int not null,
warehouse_id tinyint,
docnum varchar(20),
docdate datetime,
supplier_id tinyint,
amount float,
status varchar(20),
status_name varchar(20),
line_id int,
item_id int,
quantity float,
price float,
amount3 float,
expir_date varchar(80),
primary key (MyPrimId)
);

alter procedure OpenXmlReturns @xmlWay nvarchar(300) 
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
set @count_old = (select count(*) from Returns) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/returns/return/items/item/line_id',2) 
with (
  id int '../../../id',
warehouse_id tinyint '../../../warehouse_id',
docnum varchar(20) '../../../docnum',
docdate datetime '../../../docdate' ,
supplier_id tinyint '../../../supplier_id',
amount float '../../../amount' ,
status varchar(20) '../../../status' ,
status_name varchar(20) '../../../status_name',
line_id int '.',
item_id int '../item_id',
quantity float '../quantity',
price float '../price',
amount3 float '../amount',
expir_date varchar(80) '../expir_date')
declare @maxdate datetime = (select max(docdate) from #temp)
declare @mindate datetime = (select min(docdate) from #temp)
delete from Returns where docdate between @mindate and @maxdate
insert into Returns (id ,
warehouse_id ,
docnum ,
docdate ,
supplier_id ,
amount ,
status ,
status_name ,
line_id ,
item_id ,
quantity ,
price ,
amount3 ,
expir_date )
select id ,
warehouse_id ,
docnum ,
docdate ,
supplier_id ,
amount ,
status ,
status_name ,
line_id ,
item_id ,
quantity ,
price ,
amount3 ,
expir_date from #temp
execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Returns) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlReturns','Returns',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlReturns','Returns',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\returns_new.xml' ,single_blob) as x
exec OpenXmlReturns 'C:\Users\DELL\Desktop\virtualpos\returns.xml'

truncate table Stock
select * from Returns

create clustered index CIndexReturns
on Returns(id) on PartitionSchemeReturns(docdate)

create partition function PartitionFunckReturns (datetime) as range 
for values ('1.1.2019')

create partition scheme PartitionSchemeReturns  as partition PartitionFunckReturns
to ([Быстрые таблицы],[Быстрые таблицы])
