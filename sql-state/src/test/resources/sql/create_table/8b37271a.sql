-- file:inherit.sql ln:716 expect:true
create table pp_recpart (a pp_rectype) partition by list (a)
