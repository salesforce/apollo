-- file:create_view.sql ln:564 expect:true
create view tt21v as
select * from tt5 natural inner join tt6
