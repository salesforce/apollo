-- file:xml.sql ln:181 expect:true
SELECT xpath('//loc:piece', '<local:data xmlns:local="http://127.0.0.1" xmlns="http://127.0.0.2"><local:piece id="1"><internal>number one</internal><internal2/></local:piece><local:piece id="2" /></local:data>', ARRAY[ARRAY['loc', 'http://127.0.0.1']])
