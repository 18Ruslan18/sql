use testdb
drop table goods
truncate table goods
create table goods(
id int not null,
external_id int,
name varchar(200),
description varchar(max),
article varchar(200),
enabled int,
sales_weight int,
volume float,
manufacturer_id int,
type varchar(30),
weight_good_flag varchar(30),
not_show_in_shop int,
html_template_id int,
group_ids varchar(200),
group_ext_ids int,
barcodes varchar(200),
vat_percent int,
created_date datetime,
last_update_date datetime,
attribute1 varchar(30),
attribute2 varchar(30),
attribute3 varchar(30),
attribute4 varchar(30),
attribute5 varchar(30),
attribute6 varchar(30),
attribute7 varchar(30),
attribute8 varchar(30),
attribute9 varchar(30),
attribute10 varchar(30),
attribute11 varchar(30),
attribute12 varchar(30),
attribute13 varchar(30),
attribute14 varchar(30),
attribute15 varchar(30),
primary key (id)
);


alter procedure OpenXmlGoods @xmlWay nvarchar(300) 
as
begin
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
set @count_old = (select count(*) from goods) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/items/item',2) 
with (
  id int,
external_id int,
name varchar(200),
description varchar(max),
article varchar(200),
enabled int,
sales_weight int,
volume float,
manufacturer_id int,
type varchar(30),
weight_good_flag varchar(30),
not_show_in_shop int,
html_template_id int,
group_ids varchar(200),
group_ext_ids int,
barcodes varchar(200),
vat_percent int,
created_date datetime,
last_update_date datetime,
attribute1 varchar(30),
attribute2 varchar(30),
attribute3 varchar(30),
attribute4 varchar(30),
attribute5 varchar(30),
attribute6 varchar(30),
attribute7 varchar(30),
attribute8 varchar(30),
attribute9 varchar(30),
attribute10 varchar(30),
attribute11 varchar(30),
attribute12 varchar(30),
attribute13 varchar(30),
attribute14 varchar(30),
attribute15 varchar(30)
  )
  merge Goods as F using #temp  as T
 on (T.id=F.id ) when not matched by target then
 insert  (
	id ,
external_id,
name,
description,
article ,
enabled,
sales_weight ,
volume ,
manufacturer_id ,
type ,
weight_good_flag ,
not_show_in_shop ,
html_template_id ,
group_ids ,
group_ext_ids ,
barcodes,
vat_percent ,
created_date ,
last_update_date ,
attribute1 ,
attribute2 ,
attribute3 ,
attribute4 ,
attribute5 ,
attribute6 ,
attribute7 ,
attribute8 ,
attribute9 ,
attribute10 ,
attribute11 ,
attribute12 ,
attribute13 ,
attribute14 ,
attribute15 
	) values ( 
	T.id ,
T.external_id,
T.name,
T.description,
T.article ,
T.enabled,
T.sales_weight ,
T.volume ,
T.manufacturer_id ,
T.type ,
T.weight_good_flag ,
T.not_show_in_shop ,
T.html_template_id ,
T.group_ids ,
T.group_ext_ids ,
T.barcodes,
T.vat_percent ,
T.created_date ,
T.last_update_date ,
T.attribute1 ,
T.attribute2 ,
T.attribute3 ,
T.attribute4 ,
T.attribute5 ,
T.attribute6 ,
T.attribute7 ,
T.attribute8 ,
T.attribute9 ,
T.attribute10 ,
T.attribute11 ,
T.attribute12 ,
T.attribute13 ,
T.attribute14 ,
T.attribute15 
	)
 when matched and (F.external_id!=T.external_id or
F.name!=T.name or
F.description!=T.description or
F.article !=T.article or
F.enabled!=T.enabled or
F.sales_weight!=T.sales_weight or
F.volume !=T.volume or
F.manufacturer_id !=T.manufacturer_id or
F.type !=T.type or
F.weight_good_flag!=T.weight_good_flag or
F.not_show_in_shop!=T.not_show_in_shop or
F.html_template_id!=T.html_template_id or
F.group_ids !=T.group_ids or
F.group_ext_ids !=T.group_ext_ids or
F.barcodes !=T.barcodes or
F.vat_percent !=T.vat_percent or
F.created_date!=T.created_date or
F.last_update_date !=T.last_update_date or
F.attribute1 != T.attribute1 or
F.attribute2 != T.attribute2 or
F.attribute3 != T.attribute3 or
F.attribute4 != T.attribute4 or
F.attribute5 != T.attribute5 or
F.attribute6 != T.attribute6 or
F.attribute7 != T.attribute7 or
F.attribute8 != T.attribute8 or
F.attribute9 != T.attribute9 or
F.attribute10 != T.attribute10 or
F.attribute11 != T.attribute11 or
F.attribute12 != T.attribute12 or
F.attribute13!= T.attribute13  or
F.attribute14 != T.attribute14 or
F.attribute15 != T.attribute15 ) then
 update set  
F.external_id=T.external_id,
F.name=T.name,
F.description=T.description,
F.article =T.article,
F.enabled=T.enabled,
F.sales_weight=T.sales_weight ,
F.volume =T.volume,
F.manufacturer_id =T.manufacturer_id,
F.type =T.type,
F.weight_good_flag=T.weight_good_flag ,
F.not_show_in_shop=T.not_show_in_shop ,
F.html_template_id=T.html_template_id ,
F.group_ids =T.group_ids,
F.group_ext_ids =T.group_ext_ids,
F.barcodes =T.barcodes,
F.vat_percent =T.vat_percent ,
F.created_date=T.created_date ,
F.last_update_date =T.last_update_date ,
F.attribute1 = T.attribute1,
F.attribute2 = T.attribute2,
F.attribute3 = T.attribute3,
F.attribute4 = T.attribute4,
F.attribute5 = T.attribute5,
F.attribute6 = T.attribute6,
F.attribute7 = T.attribute7,
F.attribute8 = T.attribute8,
F.attribute9 = T.attribute9,
F.attribute10 = T.attribute10,
F.attribute11 = T.attribute11,
F.attribute12 = T.attribute12 ,
F.attribute13= T.attribute13  ,
F.attribute14 = T.attribute14 ,
F.attribute15 = T.attribute15 
	;

execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from goods) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlGoods','Goods',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlGoods','Goods',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

exec OpenXmlGoods 'C:\Users\DELL\Desktop\virtualpos\goods.xml'
select *from LogProcedures


declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\goods_new.xml' ,single_blob) as x
exec OpenXmlGoods @xml=@xml1

select * from goods order by id

create clustered index CIndexGoods
on goods(id) on PartitionSchemeGoods2(created_date)

create index N1IndexGoods on Goods(name)

create index N2IndexGoods on Goods(manufacturer_id)



create partition function PartitionFunckGoods1 (datetime) as range 
for values ('1.1.2018','1.1.2019')

create partition scheme PartitionSchemeGoods2  as partition PartitionFunckGoods1
to ([Медленные таблицы],[Медленные таблицы],[Медленные таблицы])