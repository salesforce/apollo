-- file:polymorphism.sql ln:703 expect:true
select * from dfunc('Hello World', c := '2009-07-25'::date, b := 20)
