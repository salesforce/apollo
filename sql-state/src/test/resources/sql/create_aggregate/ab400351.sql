-- file:aggregates.sql ln:712 expect:true
create aggregate my_avg(int4)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn
)
