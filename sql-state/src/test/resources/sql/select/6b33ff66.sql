-- file:polymorphism.sql ln:360 expect:true
select f3, myaggp09a(f1) from t group by f3 order by f3
