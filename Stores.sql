use testdb
drop table Stores
create table Stores (
open_time time not null,
close_time time not null,
id int not null,
number int,
name varchar(200) not null,
address varchar(300),
phone varchar(50),
headquerter_id int,
created_date datetime not null,
created_by int not null,
last_update_date datetime not null,
last_update_by int not null,
flag24hours bit not null,
lat float not null,
lon float not null,
minusale bit not null,
location_id int not null,
external_id int,
show_in_shop  bit not null,
organisation_id int not null,
vat_mandatory_flag bit not null,
manager_user_id int,
primar bit ,
location_name varchar(50) not null,
primary key (id)
);

alter procedure OpenXmlStores @xmlWay nvarchar(300) 
as
begin
--start: exec OpenXmlManufactures 'C:\Users\DELL\Desktop\virtualpos\manufactures_new.xml'

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
set @count_old = (select count(*) from Stores) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/warehouses/warehouse',2) 
with (
  open_time time ,
close_time time ,
id int ,
number int,
name varchar(200) ,
address varchar(300),
phone varchar(50),
headquerter_id int,
created_date datetime ,
created_by int ,
last_update_date datetime ,
last_update_by int ,
flag24hours bit ,
lat float ,
lon float ,
minusale bit ,
location_id int ,
external_id int,
show_in_shop  bit ,
organisation_id int ,
vat_mandatory_flag bit ,
manager_user_id int,
primar bit ,
location_name varchar(50))
merge Stores as F using #temp  as T
 on (T.id=F.id) when not matched by target then
 insert (open_time,
close_time  ,
id ,
number ,
name  ,
address ,
phone ,
headquerter_id ,
created_date  ,
created_by  ,
last_update_date ,
last_update_by  ,
flag24hours  ,
lat  ,
lon  ,
minusale  ,
location_id  ,
external_id ,
show_in_shop   ,
organisation_id ,
vat_mandatory_flag  ,
manager_user_id ,
primar  ,
location_name) values (T.open_time,
T.close_time  ,
T.id ,
T.number ,
T.name  ,
T.address ,
T.phone ,
T.headquerter_id ,
T.created_date  ,
T.created_by  ,
T.last_update_date ,
T.last_update_by  ,
T.flag24hours  ,
T.lat  ,
T.lon  ,
T.minusale  ,
T.location_id  ,
T.external_id ,
T.show_in_shop   ,
T.organisation_id ,
T.vat_mandatory_flag  ,
T.manager_user_id ,
T.primar  ,
T.location_name)
 when matched and (F.open_time != T.open_time or
F.close_time != T.close_time  or
F.id != T.id or
F.number != T.number or
F.name  != T.name or
F.address!= T.address or
F.phone != T.phone or
F.headquerter_id!= T.headquerter_id or
F.created_date != T.created_date or
F.created_by != T.created_by or
F.last_update_date!= T.last_update_date or
F.last_update_by != T.last_update_by or
F.flag24hours != T.flag24hours or
F.lat  != T.lat or
F.lon  != T.lon or
F.minusale != T.minusale or
F.location_id  != T.location_id or
F.external_id!= T.external_id or
F.show_in_shop  != T.show_in_shop or
F.organisation_id != T.organisation_id or
F.vat_mandatory_flag != T.vat_mandatory_flag or
F.manager_user_id != T.manager_user_id or
F.primar  != T.primar or
F.location_name != T.location_name) then
 update set 
F.open_time = T.open_time,
F.close_time= T.close_time  ,
F.id = T.id,
F.number = T.number,
F.name  = T.name,
F.address= T.address ,
F.phone = T.phone,
F.headquerter_id= T.headquerter_id ,
F.created_date = T.created_date ,
F.created_by = T.created_by ,
F.last_update_date= T.last_update_date ,
F.last_update_by = T.last_update_by ,
F.flag24hours = T.flag24hours ,
F.lat  = T.lat,
F.lon  = T.lon,
F.minusale = T.minusale ,
F.location_id  = T.location_id,
F.external_id= T.external_id ,
F.show_in_shop  = T.show_in_shop ,
F.organisation_id = T.organisation_id,
F.vat_mandatory_flag = T.vat_mandatory_flag ,
F.manager_user_id = T.manager_user_id,
F.primar  = T.primar,
F.location_name= T.location_name;

execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Stores) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlStores','Stores',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlStores','Stores',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\stores_new.xml' ,single_blob) as x
exec OpenXmlStores 'C:\Users\DELL\Desktop\virtualpos\stores.xml'

select * from Stores

create clustered index CIndexStores
on Stores(id) on [Быстрые таблицы]

create index N1InndexStores on Stores(name)
create index N2InndexStores on Stores(address)