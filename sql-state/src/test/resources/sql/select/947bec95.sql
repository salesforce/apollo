-- file:rangefuncs.sql ln:32 expect:true
select * from unnest(array[10,20],array['foo','bar'],array[1.0])
