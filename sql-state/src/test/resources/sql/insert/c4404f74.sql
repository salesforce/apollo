-- file:triggers.sql ln:955 expect:true
INSERT INTO european_city_view(city_name, country_name)
    VALUES ('Cambridge', 'USA') RETURNING *
