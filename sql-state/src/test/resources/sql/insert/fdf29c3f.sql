-- file:arrays.sql ln:132 expect:true
INSERT INTO arrtest_s VALUES ('[0:4]={1,2,3,4,5}', '[0:2][0:2]={{1,2,3}, {4,5,6}, {7,8,9}}')
