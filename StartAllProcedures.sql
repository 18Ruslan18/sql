use testdb1
alter procedure StartAllProcedures @start_date date, @end_date date 
as
begin
declare @timeStart datetime 
set @timeStart=getdate()
declare @timeEnd datetime 
begin try
exec fillingData @start_date, @end_date
exec OpenXmlGoods 'C:\Users\DELL\Desktop\virtualpos\goods.xml'
exec OpenXmlGoodgroups 'C:\Users\DELL\Desktop\virtualpos\goodgroups.xml'  
exec OpenXmlManufactures 'C:\Users\DELL\Desktop\virtualpos\manufactures.xml'
exec OpenXmlStores 'C:\Users\DELL\Desktop\virtualpos\stores.xml'
exec OpenXmlSuppliers 'C:\Users\DELL\Desktop\virtualpos\suppliers.xml'
exec OpenXmlReceipt 'C:\Users\DELL\Desktop\virtualpos\receipt.xml'
exec OpenXmlIncome 'C:\Users\DELL\Desktop\virtualpos\income.xml'
exec OpenXmlMovegood 'C:\Users\DELL\Desktop\virtualpos\movegood.xml'
exec OpenXmlReturns 'C:\Users\DELL\Desktop\virtualpos\returns.xml'
exec OpenXmlStock 'C:\Users\DELL\Desktop\virtualpos\stock.xml'
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('StartAllProcedures','All table ',SUSER_NAME(), 0,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('StartAllProcedures','All table',SUSER_NAME(), 0,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

exec StartAllProcedures '2017-01-01','2019-12-31'

select * from LogProcedures
select * from Suppliers
select * from goods
select * from Goodgroups