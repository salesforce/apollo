-- file:polymorphism.sql ln:701 expect:true
select * from dfunc(c := '2009-07-25'::date, a := 'Hello World', b := 20)
