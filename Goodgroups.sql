use testdb
create table Goodgroups (
id int not null,
name varchar(200) not null,
parent_id int,
not_show_in_shop int,
index_tree varchar(50),
created_date datetime not null,
last_update_date datetime not null,
primary key (id)
);

alter procedure OpenXmlGoodgroups @xmlWay nvarchar(300)  
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
set @count_old = (select count(*) from Goodgroups) 
--перенос данных с xml файла в таблицу
declare @xml_output int
exec sp_xml_preparedocument @xml_output output, @xml
select * into #temp from openxml(@xml_output,'/root/item_groups/item_group',2) 
with (
  id int ,
name varchar(200),
parent_id int,
not_show_in_shop int,
index_tree varchar(50),
created_date datetime ,
last_update_date datetime )
merge Goodgroups as F using #temp  as T
 on (T.id=F.id) when not matched by target then
 insert (id, name, parent_id ,not_show_in_shop , index_tree, created_date, last_update_date) 
 values (T.id,T.name,T.parent_id ,T.not_show_in_shop ,T.index_tree,  T.created_date, T.last_update_date)
 when matched and (F.name!=T.name or F.parent_id!=T.parent_id or F.not_show_in_shop!=T.not_show_in_shop) then
 update set F.name=T.name, F.parent_id=T.parent_id ,F.not_show_in_shop=T.not_show_in_shop ,
F.index_tree=T.index_tree,  F.created_date= T.created_date,F.last_update_date= T.last_update_date;
execute sp_xml_removedocument @xml_output
set @count_new = (select count(*) from Goodgroups) 
set @count_add = @count_new - @count_old
--логирование
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlGoodgroups','Goodgroups',SUSER_NAME(), @count_add,'Успех' ,getdate(),convert(time,@timeEnd-@timeStart,14),'NO')
end try
begin catch
set @timeEnd=getdate()
insert into LogProcedures (nameFunction,nameTable, username, amountRow, answerRun, timeUse, timeRun, errorMessage) values
('OpenXmlGoodgroups','Goodgroups',SUSER_NAME(), @count_add,'Провал' ,getdate(),convert(time,@timeEnd-@timeStart,14), error_message())
print 'This is the error: ' + error_message()
end catch
end

exec OpenXmlGoodgroups 'C:\Users\DELL\Desktop\virtualpos\goodgroups.xml'  
declare @xml1 XML
select @xml1 = convert(XML,bulkcolumn,2) from openrowset(bulk 'C:\Users\DELL\Desktop\virtualpos\goodgroups_new.xml' ,single_blob) as x
exec OpenXmlGoodgroups @xml=@xml1

select * from Goodgroups order by id

create clustered index CIndexGoodgroups
on Goodgroups(id) on [Быстрые таблицы]

create index  N1IndexGoodgroups on Goodgroups(name)