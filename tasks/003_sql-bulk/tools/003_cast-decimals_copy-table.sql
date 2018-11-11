-- @todo: replace db name
USE "qlik_medical";

INSERT INTO [dbo].[F_PRESCRIBING] (
	ID_AMP_NM,
	ID_VMP_NM,
	ID_VTM_NM,
	ID_INTERVAL_COST,
	ID_BNF,
	ID_PRACTICE,
	ID_TIME,
	TOTAL_ITEMS,
	TOTAL_QUANTITY,
	GROSS_COST,
	ACTUAL_COST
)
SELECT
	ID_AMP_NM,
	ID_VMP_NM,
	ID_VTM_NM,
	ID_INTERVAL_COST,
	ID_BNF,
	ID_PRACTICE,
	ID_TIME,
	TOTAL_ITEMS,
	TOTAL_QUANTITY,
	 CAST( REPLACE( GROSS_COST,',','.')  AS DECIMAL(18,3)) ,
	 CAST( REPLACE( ACTUAL_COST,',','.') AS DECIMAL(18,3))

FROM [dbo].[F_PRESCRIBING__temp];