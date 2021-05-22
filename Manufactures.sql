use testdb
drop table Manufactures
create table Manufactures(
id int not null,
name varchar(200) not null,
created_date datetime not null,
last_update_date datetime not null,
primary key (id)
) ;

---'C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml'

alter procedure OpenXmlManufactures @xmlWay nvarchar(300) 
as
begin
--start: exec OpenXmlManufactures 'C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml'
--перменные для замера времени выполнения процедуры
declare @timeStart datetime 
set @timeStart=getdate()
declare @timeEnd datetime 
--declare @xml2 xml
--set @xml2 =replace((select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
--,codepage='ANSI',single_clob)  x) ,'encoding="UTF-8"','encoding="Windows-1252"')
begin try
declare @xml XML
declare @find nvarchar(50) = 'encoding="UTF-8"'
declare @replacement nvarchar(50) = 'encoding="Windows-1251"'
  declare @sql nvarchar(max) = N'set @xml= replace((select BulkColumn from openrowset(bulk  '''+ @xmlWay+'''
,single_clob)  x),'''+@find+''','''+@replacement+''');';
exec sp_executesql @sql, N'@xml xml OUTPUT', @xml OUTPUT;
--declare @x int
--set @x = 1/0
--переменые для подсчета кол-ва добавленных строк
declare @count_new int
declare @count_old int
declare @count_add int
set @count_old = (select count(*) from Manufactures) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/manufacturers/manufacturer',2) 
with (
  id int , name varchar(200), created_date datetime, last_update_date datetime)
 --select * from #temp
 merge Manufactures as F using #temp  as T
 on (T.id=F.id ) when not matched by target then
 insert (id, name, created_date, last_update_date) values (T.id,T.name, T.created_date, T.last_update_date)
 when matched and (F.name!=T.name or F.created_date!= T.created_date or F.last_update_date!= T.last_update_date) then
 update set F.name=T.name, F.created_date= T.created_date,F.last_update_date= T.last_update_date;
--insert into Manufactures (id,name,created_date,last_update_date)
--select id,name, created_date, last_update_date from #temp
execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Manufactures) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlManufactures','Manufactures',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlManufactures','Manufactures',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

exec OpenXmlManufactures 'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
select *from LogProcedures
---merge-----
select * from Manufactures 
select * from Manufactures_to
 
 insert into Manufactures_to (id, name, created_date, last_update_date) values (10000,'eddq',getdate(),getdate())
 update Manufactures set  Manufactures.name='Bon' where Manufactures.id=2634

 merge Manufactures as F using Manufactures_to  as T
 on (T.id=F.id) when not matched by target then
 insert (id, name, created_date, last_update_date) values (T.id,T.name, T.created_date, T.last_update_date)
 when matched then
 update set F.name=T.name, F.created_date= T.created_date,F.last_update_date= T.last_update_date;

 ---------------way file in parametr procedure-----------------
 declare @xml1 XML
 declare @xmlWay nvarchar(300)=N'C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml'
 exec sp_executesql N' select @ixml1= convert(XML,bulkcolumn,2) from openrowset(bulk  '''+ @ixmlWay+''',
single_clob)  as x',N'@ixmlWay nvarchar(300),@ixml1 xml OUTPUT', @ixmlWay=@xmlWay, @ixml1=@xml1 OUTPUT;

 declare @xmlWay nvarchar(300)='C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml'
  declare @xml1 XML
  declare @sql nvarchar(max) = N'select @xml1= convert(XML,bulkcolumn,2) from openrowset(bulk  '''+ @xmlWay+''',
single_clob)  xml;';
exec sp_executesql @sql, N'@xml1 xml OUTPUT', @xml1 OUTPUT;

 declare @xml1 XML
select @xml1= convert(XML,bulkcolumn,2) from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml',
single_clob)  as x
 --------------------------
create clustered index CIndexManufactures
on Manufactures(id) with drop_existing on [Быстрые таблицы]
 select SUSER_NAME()

 create index N1IndexManufactures on Manufactures(name)

  ----------------------------------------------
 
 
 
-----------encoding-----------

---powershell Get-Content -Encoding UTF8 "$path\Users\DELL\Desktop\virtualpos\manufactures.xml" 
---| Set-Content -Encoding Unicode "$path\Users\DELL\Desktop\virtualpos\manufactures.xml"

declare @tmp nvarchar(max)
set @tmp = convert(nvarchar(max),(select BulkColumn from openrowset(bulk  N'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
,codepage='UTF-8',single_clob)  x) )

print @tmp
declare @xml21 xml
set @xml21 = convert(xml,@tmp)



declare @xml2 xml
set @xml2 =replace(convert(varchar(41750528),(select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
,codepage='ANSI',single_clob)  x) ),'encoding="UTF-8"','encoding="Windows-1252"')
print @xml2


declare @xml2 xml
set @xml2 =replace((select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
,codepage='ANSI',single_clob)  x) ,'encoding="UTF-8"','encoding="Windows-1252"')
print @xml2

declare @xml2 xml
set @xml2 = (select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml',
codepage='UTF-8',single_clob)  x)

declare @txtxml nvarchar(max)= convert(varchar(max),(select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
,codepage='UTF-8',single_clob)  x) )

declare @txtxmlnew  nvarchar(max)= convert(varchar(max),(select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml'
,codepage='_UTF8',single_clob)  x) )
if @txtxml=@txtxmlnew print 'true' else print 'false' 


declare @xml_output1 int
exec sp_xml_preparedocument @xml_output1 output, (select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml',
single_clob)  x) 
select * into TempManufactures from openxml(@xml_output1,'/root/manufacturers/manufacturer',2) 


declare @xml1 XML
create view MyView1 as
select Xml.BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml',
single_clob)  xml
 declare @xml1 XML


exec OpenXmlManufactures @xml = (select Xml.BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml',
single_clob)  xml)

declare @xml_output int
exec sp_xml_preparedocument @xml_output output, (select Xml.BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml',
single_clob))


drop table #temp


insert into TempManufactures (
  id, name, created_date, last_update_date) 
  
  select  id, name, created_date, last_update_date  from (select BulkColumn from openrowset(bulk  'C:\Users\DELL\Desktop\virtualpos\manufactures.xml',
single_clob)  xml)

drop table TempManufactures
create table TempManufactures (
id int not null,
name varchar(200) not null,
created_date datetime not null,
last_update_date datetime not null,
primary key (id)
) ;

BULK INSERT TempManufactures 
FROM 'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
WITH (CODEPAGE = '65001', DATAFILETYPE = 'Char')
