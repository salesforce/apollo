-- file:jsonb.sql ln:371 expect:true
SELECT jsonb_build_object('turbines',jsonb_object_agg(serial_num,jsonb_build_object('name',name,'type',type)))
FROM foo
