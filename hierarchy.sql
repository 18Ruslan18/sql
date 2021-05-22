use testdb1 
select count(*) from Receipt group by receipt_id
select * from goods 
select * from Goodgroups where id = 219
select sum(amount3) from Receipt


with cte as (
select id,name,parent_id, 1 as level
from Goodgroups where id = 117
union all 
select h.id,h.name,h.parent_id, t.level+1 
from cte t inner join
Goodgroups h on t.id =h.parent_id)
select * from cte


select * from goods where group_ids like '117,119%' 

select * from Receipt where doc_type = 'return'

update Receipt set amount3=-amount3 where doc_type = 'return'