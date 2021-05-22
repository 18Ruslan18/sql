alter function ReturnGoodGroup(@group_ids varchar(100)) returns int
as 
begin
if (@group_ids=null) return null
declare @response varchar(5)
set @response = right(@group_ids,3)
if (left(@response,1)=',') set @response = right(@group_ids,2)
if (right(left(@response,2),1)=',') set @response = right(@group_ids,1)
return convert(int,@response)
end
go
select dbo.ReturnGoodGroup('6343.4')
select dbo.ReturnGoodGroup(group_ids),id from goods  order by len(dbo.ReturnGoodGroup(group_ids)) 
select * from goods where id = 24194

select * from Goodgroups