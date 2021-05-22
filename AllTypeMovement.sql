use testdb1
select * from Receipt where amount3 < 0
select * from goods where name = 'ÈÍÒÅÐÍÅÒ-ÇÀÊÀÇ ÊÀÒÐÅÍ'

update goods set group_ids='993' where name = 'ÈÍÒÅÐÍÅÒ-ÇÀÊÀÇ ÊÀÒÐÅÍ'
--dbo.ReturnGoodGroup(group_ids)
select * from Income where status = 'draft'
select * from movegood
select * from Returns
update Goodgroups set id = 993 where name='Èíòåðíåò-çàêàç'
select * from Goodgroups where id = 0
select * from Goodgroups where name = 'àíåìèè'
insert into Goodgroups values(0,'Èíòåðíåò-çàêàç',0,0,'0:', getdate(),getdate())
select goods.id, goods.name, dbo.ReturnGoodGroup(group_ids), income.warehouse_id, Income.status, movegood.status from goods
right join Income on goods.id = income.item_id   
full join movegood on goods.id=movegood.item_id
full join Returns on goods.id=returns.item_id


select * from goods

delete income where id != in  (select id from Goodgroups)

alter view AllTypeMovement as
select goods.id, goods.name, dbo.ReturnGoodGroup(group_ids) as goodGroup, doc_type as status, 
quantity,cogs as price,amount3 as amount, 'receipt' as type, 
date as data, 
warehouseid  as warehouse_id, 1001 as src_warehouse_id, 1001 as dst_warehouse_id
from goods
right join Receipt on goods.id =Receipt.itemid  where goods.id is not null
	union all
select goods.id, goods.name, dbo.ReturnGoodGroup(group_ids) as goodGroup, status, 
quantity,price,amount, 'income' as type, 
docdate as data, 
warehouse_id, 1001 as src_warehouse_id, 1001 as dst_warehouse_id
from goods
right join Income on goods.id = income.item_id  where goods.id is not null
	union all
select goods.id, goods.name, dbo.ReturnGoodGroup(group_ids)  as goodGroup, status, 
quantity,price,amount, 'movegood' as type, 
movegood.created_date as data, 
1000 as warehouse_id,src_warehouse_id, dst_warehouse_id  
from goods
right join movegood on goods.id = movegood.item_id   
	union all
select goods.id, goods.name, dbo.ReturnGoodGroup(group_ids)  as goodGroup, status, 
quantity,price,amount, 'returns' as type,  
docdate as data, 
warehouse_id , 1001 as src_warehouse_id, 1001 as dst_warehouse_id
  from goods
right join Returns on goods.id = Returns.item_id  


update Receipt set remainder = -quantity where doc_type = 'sale'   
update Receipt set remainder = quantity where doc_type = 'return'   
select * from AllTypeMovement where name = 'ÈÍÒÅÐÍÅÒ-ÇÀÊÀÇ ÊÀÒÐÅÍ'  
update income set goodGroup=993 where goodGroup=0   

select * from AllTypeMovement where type='returns'
--ÈÍÒÅÐÍÅÒ-ÇÀÊÀÇ ÊÀÒÐÅÍ ÃÎÌÅÎÔÀÐÌ ÈÍÒÅÐÍÅÒ-ÇÀÊÀÇ ÈÍÒÅÐÍÅÒ-ÇÀÊÀÇ ÏÐÎÒÅÊ
delete AllTypeMovement where id = null
select * from TypeMovement
create table TypeMovement (
id varchar(10)
primary key (id)
)
insert TypeMovement values ('income')
insert TypeMovement values ('movegood')
insert TypeMovement values ('returns')
insert TypeMovement values ('receipt')
 
 select * from StatusMovement
create table StatusMovement
(
id varchar(10)
primary key (id)
)
insert StatusMovement values ('accept')
insert StatusMovement values ('complete')
insert StatusMovement values ('draft')
insert StatusMovement values ('send')


select sum(quantity) from AllTypeMovement

select * from stores
insert into Stores (0,0,1000,0,'-',0,  