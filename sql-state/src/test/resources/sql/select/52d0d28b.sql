-- file:interval.sql ln:157 expect:true
SELECT '1 millisecond'::interval, '1 microsecond'::interval,
       '500 seconds 99 milliseconds 51 microseconds'::interval
