-- file:arrays.sql ln:103 expect:true
select '[0:2][0:2]={{1,2,3},{4,5,6},{7,8,9}}'::int[]
