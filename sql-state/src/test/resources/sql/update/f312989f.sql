-- file:arrays.sql ln:72 expect:true
UPDATE arrtest
  SET b[1:1][1:1][1:2] = '{113, 117}',
      b[1:1][1:2][2:2] = '{142, 147}'
  WHERE array_dims(b) = '[1:1][1:2][1:2]'
