-- file:polymorphism.sql ln:363 expect:true
select f3, myaggp10b(f1) from t group by f3 order by f3
