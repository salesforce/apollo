-- file:insert.sql ln:270 expect:true
alter table mlparted attach partition mlparted5 for values from (1, 40) to (1, 50)
