CREATE OR REPLACE FUNCTION keepcoding.clean_integer(null_int int64)
RETURNS int64 AS (
  CASE
    WHEN null_int IS NULL THEN -999999
    ELSE null_int
  END
);