-- file:bitmapops.sql ln:17 expect:true
INSERT INTO bmscantest
  SELECT (r%53), (r%59), 'foooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo'
  FROM generate_series(1,70000) r
