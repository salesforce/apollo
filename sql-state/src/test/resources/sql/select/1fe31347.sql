-- file:jsonb.sql ln:1019 expect:true
select jsonb_set('{"a":1,"b":[0,1,2],"c":{"d":4}}','{x,y}','{"foo":123}')
