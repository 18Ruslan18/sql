use testdb
truncate table Receipt 
drop table Receipt
create table Receipt (

receipt_id varchar(80) not null,
marker varchar(65),
terminalid bigint,
warehouseid int,
user_id int,
fr_session int,
doc_type varchar(10),
doc_num int,
doc_num_session int,
date varchar(80),
discount float,
discount_misc float,
amount float,
round_amount float,
pay_cash float,
pay_card float,
pay_tare float,
pay_credit float,
pay_bonus_amount float,
pinpad_name varchar(30),
terminal_number tinyint,
short_fiscal_serial_number int,
terminal_description varchar (30),
is_electronic tinyint,
fiscal_attribute bigint,
vat10_sum float,
vat18_sum float,
vat20_sum float,
items_count tinyint,
line_id varchar(70),
itemid int,
quantity float,
pricebase float,
pricesale float,
discount2 float,
amount3 float,
barcode varchar(25),
cogs float,
vat_applied float,
vat_amount float,
onhand_id int
);

alter procedure OpenXmlReceipt @xmlWay nvarchar(300) 
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
set @count_old = (select count(*) from Receipt) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/receipts/receipt/items/item/line_id',2) 
with (
  receipt_id varchar(80) '../../../receipt_id' ,
marker varchar(65) '../../../marker' ,
terminalid bigint '../../../terminalid' ,
warehouseid int '../../../warehouseid' ,
user_id int '../../../user_id',
fr_session int '../../../fr_session',
doc_type varchar(10) '../../../doc_type',
doc_num int '../../../doc_num',
doc_num_session int '../../../doc_num_session',
date varchar(80) '../../../date',
discount float '../../../discount',
discount_misc float '../../../discount_misc',
amount float '../../../amount',
round_amount float '../../../round_amount',
pay_cash float '../../../pay_cash',
pay_card float '../../../pay_card',
pay_tare float '../../../pay_tare',
pay_credit float '../../../pay_credit',
pay_bonus_amount float '../../../pay_bonus_amount',
pinpad_name varchar(30) '../../../pinpad_name',
terminal_number tinyint '../../../terminal_number',
short_fiscal_serial_number int '../../../short_fiscal_serial_number',
terminal_description varchar (30) '../../../terminal_description',
is_electronic tinyint '../../../is_electronic' ,
fiscal_attribute bigint '../../../fiscal_attribute',
vat10_sum float '../../../vat10_sum' ,
vat18_sum float '../../../vat18_sum',
vat20_sum float '../../../vat20_sum',
items_count tinyint '../../../items_count',
line_id varchar(70) '.' ,
itemid int '../itemid',
quantity float '../quantity',
pricebase float '../pricebase ',
pricesale float '../pricesale',
discount2 float '../discount ',
amount3 float '../amount',
barcode varchar(25) '../barcode',
cogs float '../cogs',
vat_applied float '../vat_applied',
vat_amount float '../vat_amount',
onhand_id int '../onhand_id')
declare @maxdate datetime = (select max(convert(datetime,date)) from #temp)
declare @mindate datetime = (select min(convert(datetime,date)) from #temp)
delete from Receipt where convert(datetime,date) between @mindate and @maxdate
insert into Receipt (receipt_id ,
marker ,
terminalid ,
warehouseid ,
user_id ,
fr_session ,
doc_type ,
doc_num ,
doc_num_session ,
date ,
discount ,
discount_misc ,
amount ,
round_amount ,
pay_cash ,
pay_card ,
pay_tare ,
pay_credit ,
pay_bonus_amount ,
pinpad_name ,
terminal_number ,
short_fiscal_serial_number ,
terminal_description ,
is_electronic ,
fiscal_attribute ,
vat10_sum ,
vat18_sum ,
vat20_sum ,
items_count ,
line_id ,
itemid ,
quantity ,
pricebase ,
pricesale ,
discount2 ,
amount3 ,
barcode ,
cogs ,
vat_applied ,
vat_amount ,
onhand_id)
select receipt_id ,
marker ,
terminalid ,
warehouseid ,
user_id ,
fr_session ,
doc_type ,
doc_num ,
doc_num_session ,
date ,
discount ,
discount_misc ,
amount ,
round_amount ,
pay_cash ,
pay_card ,
pay_tare ,
pay_credit ,
pay_bonus_amount ,
pinpad_name ,
terminal_number ,
short_fiscal_serial_number ,
terminal_description ,
is_electronic ,
fiscal_attribute ,
vat10_sum ,
vat18_sum ,
vat20_sum ,
items_count ,
line_id ,
itemid ,
quantity ,
pricebase ,
pricesale ,
discount2 ,
amount3 ,
barcode ,
cogs ,
vat_applied ,
vat_amount ,
onhand_id from #temp
execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Receipt) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlReceipt','Receipt',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlReceipt','Receipt',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\receipt_new.xml' ,single_blob) as x
exec OpenXmlReceipt 'C:\Users\DELL\Desktop\virtualpos\receipt.xml'

select * from Receipt

create clustered index CIndexReceipt
on Receipt(receipt_id) on PartitionSchemeReceipt1(date)

create partition function PartitionFunckReceipt1 (varchar(80)) as range 
for values ('1.1.2019')

create partition scheme PartitionSchemeReceipt1  as partition PartitionFunckReceipt1
to ([Быстрые таблицы],[Быстрорастущие таблицы])

create index N1IndexReceipt on Receipt(itemid)

create index N2IndexReceipt on Receipt(warehouseid)

create index N3IndexReceipt on Receipt(user_id)

create index N4IndexReceipt on Receipt(onhand_id)

