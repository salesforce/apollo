-- file:jsonb.sql ln:251 expect:true
SELECT jsonb_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":"null"}')
