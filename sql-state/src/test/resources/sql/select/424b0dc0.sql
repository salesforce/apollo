-- file:jsonb.sql ln:788 expect:true
SELECT '{"aa":["a","aaa"],"qq":{"a":"12","b":"16","c":["c1","c2",["c3"],{"c4":4}],"d":{"d1":"d1","d2":"d2"}}}'::jsonb
