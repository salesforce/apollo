-- file:create_am.sql ln:58 expect:true
SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box
