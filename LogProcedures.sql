use testdb
drop table LogProcedures
create table LogProcedures (
id int identity(1,1),
nameFunction varchar(200) not null,
nameTable varchar(200) not null,
username varchar(200) not null,
amountRow int,
answerRun varchar(10) not null,
timeUse datetime not null,
timeRun time,
errorMessage varchar(200)
primary key (id)
)

select * from LogProcedures