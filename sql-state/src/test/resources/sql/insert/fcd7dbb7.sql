-- file:updatable_views.sql ln:1182 expect:true
insert into uv_iocu_view (a, b) values ('xyxyxy', 1)
   on conflict (a) do update set b = excluded.b
