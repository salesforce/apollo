-- file:triggers.sql ln:895 expect:true
INSERT INTO city_view(city_name, country_name) VALUES('Washington DC', 'USA') RETURNING *
