-- @todo: replace the db name
USE "qlik_medical";

INSERT D_TIME( [MONTH], [YEAR])
  SELECT [MONTH],[YEAR] 
  FROM D_TIME__temp;