use testdb
create table Suppliers (
id int not null,
name varchar(200) not null,
legal_name varchar(100),
inn varchar(20),
kpp varchar(20),
address varchar(300),
phone varchar(50),
email varchar(50),
created_date datetime not null,
created_by int not null,
last_update_date datetime not null,
last_update_by int not null
primary key (id)
);

alter procedure OpenXmlSuppliers @xmlWay nvarchar(300) 
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
set @count_old = (select count(*) from Suppliers) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/suppliers/supplier',2) 
with (
  id int ,
name varchar(200) ,
legal_name varchar(100),
inn varchar(20),
kpp varchar(20),
address varchar(300),
phone varchar(50),
email varchar(50),
created_date datetime ,
created_by int,
last_update_date datetime,
last_update_by int )
merge Suppliers as F using #temp  as T
 on (T.id=F.id) when not matched by target then
 insert ( id  ,
name  ,
legal_name,
inn ,
kpp ,
address ,
phone ,
email ,
created_date  ,
created_by ,
last_update_date ,
last_update_by) values ( T.id  ,
T.name  ,
T.legal_name,
T.inn ,
T.kpp ,
T.address ,
T.phone ,
T.email ,
T.created_date  ,
T.created_by ,
T.last_update_date ,
T.last_update_by)
 when matched and (
F.name != T.name or
F.legal_name != T.legal_name or
F.inn != T.inn or
F.kpp != T.kpp or
F.address != T.address or
F.phone != T.phone or
F.email != T.email or
F.created_date != T.created_date or
F.created_by != T.created_by or
F.last_update_date!= T.last_update_date or
F.last_update_by!= T.last_update_by) then
 update set 
F.name = T.name ,
F.legal_name= T.legal_name,
F.inn = T.inn,
F.kpp = T.kpp,
F.address= T.address ,
F.phone = T.phone,
F.email = T.email,
F.created_date = T.created_date ,
F.created_by = T.created_by,
F.last_update_date= T.last_update_date ,
F.last_update_by= T.last_update_by;

execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Suppliers) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlSuppliers','Suppliers',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlSuppliers','Suppliers',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\suppliers_new.xml' ,single_blob) as x
exec OpenXmlSuppliers 'C:\Users\DELL\Desktop\virtualpos\suppliers.xml'

select * from Suppliers

create clustered index CIndexSuppliers
on Suppliers(id) on [Быстрые таблицы]

create index N1IndexSuppliers on Suppliers(name)

create index N2IndexSuppliers on Suppliers(legal_name)

create index N3IndexSuppliers on Suppliers(address)