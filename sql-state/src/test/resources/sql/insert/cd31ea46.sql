-- file:box.sql ln:127 expect:true
INSERT INTO box_temp
	SELECT box(point(i, i), point(i * 2, i * 2))
	FROM generate_series(1, 50) AS i
