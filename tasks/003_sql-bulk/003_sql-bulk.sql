-- @todo: replace db name
use "qlik_medical";


-- @todo: replace absolute path to file and import parameters
bulk insert [dbo].[F_PRESCRIBING__temp]
from "C:\filename.csv"
with(
	rowterminator='\n',
	fieldterminator=';',
	firstrow = 2,
	keepnulls
)