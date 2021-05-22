use testdb
drop table data
create table data (
id int identity(1,1) not null,
year int not null,
quarter int not null,
month int not null,
week int not null,
day int not null,
dateColumn date not null,
yearMonthInt int not null,
dateInt int  not null,
primary key (id))

alter procedure fillingData @start_date date, @end_date date 
as
begin
truncate table data
declare @counter date =  @start_date
while @counter<=@end_date
begin
insert data (year,quarter,month,week,day,dateColumn,yearMonthInt,dateInt) values (datepart(YY,@counter),datepart(QQ,@counter),
datepart(MM,@counter),datepart(WW,@counter),datepart(DD,@counter),@counter,concat(datepart(YY,@counter),
format(@counter,'MM')),concat(datepart(YY,@counter),
format(@counter,'MM'),format(@counter,'dd')) )
set @counter=dateadd(day,1,@counter)
end
end

exec fillingData '2017-01-01','2019-12-31'
select * from data
declare @y int = 2021 
declare @m int = 10 
declare @d int = 1
declare @ee int =concat(@y,@m,@d)
print @ee
declare @cha nvarchar = convert(nvarchar,@y) ---+convert(varchar,@m)+convert(varchar,@d)
print @cha
