-- file:triggers.sql ln:896 expect:true
INSERT INTO city_view(city_id, city_name) VALUES(123456, 'New York') RETURNING *
