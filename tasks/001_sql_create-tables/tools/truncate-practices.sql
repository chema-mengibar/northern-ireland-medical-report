-- @todo: replace the db name
USE "qlik_medical";

-- ALTER TABLE [dbo].[F_PRESCRIBING] DROP CONSTRAINT F_PRESCRIBING_FK4;

-- ALTER TABLE [dbo].[D_PRACTICE] DROP CONSTRAINT D_PRACTICE_PK;
-- ALTER TABLE [dbo].[D_PRACTICE] DROP CONSTRAINT D_PRACTICE_FK1;



-- TRUNCATE TABLE [D_PRACTICE];



ALTER TABLE [dbo].D_PRACTICE
	ADD CONSTRAINT D_PRACTICE_PK
		PRIMARY KEY ("ID_PRACTICE");

ALTER TABLE [dbo].D_PRACTICE
	ADD CONSTRAINT D_PRACTICE_FK1
		FOREIGN KEY ("ID_POSTCODE") REFERENCES [dbo].[A_POSTCODE]("ID_POSTCODE");

ALTER TABLE [dbo].F_PRESCRIBING
	ADD CONSTRAINT F_PRESCRIBING_FK4
		FOREIGN KEY ("ID_PRACTICE") REFERENCES [dbo].[D_PRACTICE]("ID_PRACTICE");
