-- file:arrays.sql ln:102 expect:true
select ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2]
