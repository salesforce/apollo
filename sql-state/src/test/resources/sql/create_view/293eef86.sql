-- file:updatable_views.sql ln:1159 expect:true
create view wcowrtest_v2 as
    select *
      from wcowrtest r
      where r in (select s from sometable s where r.a = s.a)
with check option
