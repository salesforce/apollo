-- file:domain.sql ln:327 expect:true
ALTER DOMAIN things ADD CONSTRAINT meow CHECK (VALUE < 11) NOT VALID
