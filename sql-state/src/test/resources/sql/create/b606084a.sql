-- file:functional_deps.sql ln:151 expect:true
CREATE TEMP VIEW fdv1 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id
