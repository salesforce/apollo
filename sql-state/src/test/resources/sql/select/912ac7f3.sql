-- file:aggregates.sql ln:90 expect:true
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four)
