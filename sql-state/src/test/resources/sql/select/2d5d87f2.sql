-- file:box.sql ln:213 expect:true
SELECT count(*) FROM quad_box_tbl WHERE b &<  box '((100,200),(300,500))'
