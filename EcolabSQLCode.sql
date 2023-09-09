
CREATE TABLE DataIngestionLog(
PIPELINE NVARCHAR(250) NOT NULL,
DESCRIPTION NVARCHAR (MAX) NOT NULL)

CREATE TABLE [dbo].[ActualPlan](
	[ActualPlanID] [int] IDENTITY(1,1) NOT NULL,
	[ActualPlan] [nvarchar](250) NOT NULL,
 CONSTRAINT [ActualPlann] PRIMARY KEY ([ActualPlanID]));

 Create Table ECLYearr(
ECLYearID INT IDENTITY(1,1),
ECLYear NVARCHAR(50) NOT NULL 
CONSTRAINT ECLYear PRIMARY KEY ( ECLYearID));


CREATE TABLE PAC(
PACCODEID INT IDENTITY(1,1),
PACCode NVARCHAR(250) NOT NULL
CONSTRAINT PACC PRIMARY KEY (PACCODEID));

CREATE TABLE SalesStage(
SalesStageID INT IDENTITY(1,1),
SalesStage NVARCHAR(250) NOT NULL
CONSTRAINT SalesStag PRIMARY KEY (SalesStageID));

CREATE TABLE OODays(
OODaysID INT IDENTITY (1,1),
OODays NVARCHAR(250) NOT NULL
CONSTRAINT OODay PRIMARY KEY (OODaysID));


CREATE TABLE Competitors(
CompetitorsID INT IDENTITY (1,1),
Competitors NVARCHAR(250) NOT NULL
CONSTRAINT Competitor PRIMARY KEY (CompetitorsID));

Create Table EcoDate (
DateID INT IDENTITY(1,1),
ActualCloseDate DATE NOT NULL, 
EstimatedCloseDate DATE NOT NULL, 
ShipmentDate DATE NOT NULL, 
Month INT NOT NULL, 
YEAR INT NOT NULL, 
Quarter INT NOT NULL,
CONSTRAINT EcolabDate PRIMARY KEY (DateID));

Create TABLE EcoLOCATION(
DISTRICT NVARCHAR(250),
CITY NVARCHAR(250) NOT NULL, 
STATEPROVINCE NVARCHAR(250) NOT NULL, 
COUNTRY NVARCHAR(250) NOT NULL,
COUNTRYCLUSTER NVARCHAR(250) NOT NULL,
COUNTRYUPDATE NVARCHAR(250) NOT NULL, 
REGION NVARCHAR(250) NOT NULL
CONSTRAINT EcolabLocation PRIMARY KEY (DISTRICT));

CREATE TABLE TechnologyPlatform(
TechnologyID INT IDENTITY(1,1),
TechPlatform NVARCHAR(MAX) NOT NULL, 
DigitalEnablers NVARCHAR(MAX) NOT NULL, 
Innovation NVARCHAR(MAX) NOT NULL, 
Digital NVARCHAR(MAX) NOT NULL,
TechnologyPlatform NVARCHAR(MAX) NOT NULL
CONSTRAINT TechPlatform PRIMARY KEY (TechnologyID));

Create TABLE CustAccount(
AccountID INT IDENTITY(1,1),
AccountNumber INT NOT NULL, 
AccountName NVARCHAR(MAX) NOT NULL, 
CorporateAccount NVARCHAR(MAX) NOT NULL,
AcctType NVARCHAR(MAX) NOT NULL,
CONSTRAINT CustomerAccount PRIMARY KEY (AccountID));

Create Table SalesPerson(
SalesPersonID INT IDENTITY(1,1),
TeamMember NVARCHAR(MAX) NOT NULL, 
AccountOwner NVARCHAR(MAX) NOT NULL,
CorporateManager NVARCHAR(MAX) NOT NULL, 
Salesman NVARCHAR(MAX) NOT NULL, 
CAMLeader NVARCHAR(MAX) NOT NULL
CONSTRAINT SalesPersonn PRIMARY KEY (SalesPersonID));

Create Table Opportunity(
OpportunityID INT IDENTITY (1,1),
OpportunityStatus NVARCHAR(MAX) NOT NULL,
OpportunityName NVARCHAR(MAX) NOT NULL, 
OpportunityType NVARCHAR(MAX) NOT NULL, 
ShipName NVARCHAR(MAX) NOT  NULL
CONSTRAINT OpenOpp PRIMARY KEY (OpportunityID));

Create Table Industry(
IndustryID INT IDENTITY(1,1),
Industry NVARCHAR(MAX) NOT NULL, 
SBU NVARCHAR(10) NOT NULL, 
IndustryCode NVARCHAR(250) NOT NULL, 
SINCDesc NVARCHAR (MAX) NOT NULL
CONSTRAINT EcoIndustry PRIMARY KEY (IndustryID));

CREATE TABLE [dbo].[Product](
	[ProductID] [int] IDENTITY(1,1) NOT NULL,
	[Product] [nvarchar](250) NOT NULL,
	[ProductDesc] [nvarchar](250) NOT NULL,
	[ProductCategory] [nvarchar](250) NOT NULL,
 CONSTRAINT [Productt] PRIMARY KEY CLUSTERED 
(
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO



CREATE Table Fact (
ActualRevenueUSD DECIMAL (10,2), 
Plan2021 DECIMAL (10,2),
RevenueEstimateUSD DECIMAL (10,2), 
SumOfRevKUSD DECIMAL (10,2),
RevenueperGM DECIMAL (10,2),
RevenueMillions DECIMAL (10,2),
[ActualPlanID] [int] NOT NULL,
CompetitorsID INT NOT NULL,
DATEID INT NOT NULL, 
DISTRICT NVARCHAR(250) NOT NULL,
TechnologyID int NOT NULL,
AccountID INT NOT NULL,
SalesPersonID INT NOT NULL,
OpportunityID INT NOT NULL,
IndustryID INT NOT NULL,
ProductID INT NOT NULL, 
PACCodeID INT NOT NULL,
OODaysID INT NOT NULL, 
SalesStageID INT NOT NULL,
ECLYearID INT NOT NULL,
ActualCloseDateID INT NOT NULL,
EstimatedCloseDateID INT NOT NULL,
FOREIGN KEY (ActualPlanID) REFeRENCES ActualPlan(ActualPlanID),
FOREIGN KEY (CompetitorsID) REFERENCES Competitors(CompetitorsID),
FOREIGN KEY (ActualCloseDateID) REFERENCES EcoDate(DateID),
FOREIGN KEY (EstimatedCloseDateID) REFERENCES EcoDate(DateID),
FOREIGN KEY (DateID) REFERENCES EcoDate(DateID),
FOREIGN KEY (District) REFERENCES EcoLocation(District),
FOREIGN KEY (TechnologyID) REFERENCES TechnologyPlatform(TechnologyID),
FOREIGN KEY (AccountID) REFERENCES CustAccount (AccountID),
FOREIGN KEY (SalesPersonID) REFERENCES SalesPerson (SalesPersonID),
FOREIGN KEY (OpportunityID) REFERENCES Opportunity(OpportunityID),
FOREIGN KEY (IndustryID) REFERENCES Industry(IndustryID),
FOREIGN KEY (ProductID) REFERENCES Product(ProductID),
FOREIGN KEY (PACCodeID) REFERENCES PAC (PACCodeID),
FOREIGN KEY (SalesStageID) REFERENCES SalesStage(SalesStageID),
FOREIGN KEY (OODaysID) REFERENCES OODays(OODaysID),
FOREIGN KEY (ECLYearID) REFERENCES ECLYearr (ECLYearID));



SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER Procedure [dbo].[SP_LoadWaterQualityDimensionFactTables]																	
AS																	
BEGIN										
													
		BEGIN			
				INSERT INTO ECLYearr ([ECLYear], [UpdatedOn], CreatedOn)								
				SELECT DISTINCT [ECLYear], GETDATE(), GETDATE()							
				FROM STG_IMEATech vuln								
				WHERE [ECLYear] IS NOT NULL								
				AND [ECLYear] !=''	
				AND [ECLYear] !='None'	
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM ECLYearr dimECL			
					WHERE dimECL.ECLYear = vuln.ECLYear			
					)		
		END;										
												
		BEGIN			
				UPDATE ECLYearr								
				SET ECLYearr.ECLYear = r.ECLYear,
					UpdatedOn = GETDATE()
				FROM ECLYearr dimECL								
				JOIN(								
					SELECT DISTINCT [ECLYear] 						
					FROM STG_IMEATech vuln	
					) r 					
				ON r.ECLYear = dimECL.ECLYear
		END;		
		

---------------------------------------- DimOODays ----------------------------------------								
												
																											
		
												
		BEGIN			
				INSERT INTO OODays ([OODays], [UpdatedOn], CreatedOn)								
				SELECT DISTINCT [OODays], GETDATE(), GETDATE()							
				FROM STG_OO vuln								
				WHERE [OODays] IS NOT NULL								
				AND [OODays] !=''	
				AND [OODays] !='None'	
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM OODays dimOO		
					WHERE dimOO.OODays = vuln.OODays			
					)		
		END;										
													
		BEGIN			
				UPDATE OODays							
				SET OODays.OODays = r.OODays,
					UpdatedOn = GETDATE()
				FROM OODays dimOO								
				JOIN(								
					SELECT DISTINCT [OODays] 						
					FROM STG_OO vuln	
					) r 					
				ON r.OODays = dimOO.OODays
		END;		
		


---------------------------------------- DimProduct ----------------------------------------------------

	BEGIN			
				INSERT INTO Product(Product,[ProductDesc], ProductCategory,[UpdatedOn], CreatedOn)								
				SELECT DISTINCT Product,[ProductDesc], ProductCategory, GETDATE(), GETDATE()							
				FROM STG_IMEATech vuln								
				WHERE Product IS NOT NULL								
				AND Product !=''	
				AND Product !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Product dimP		
					WHERE dimP.Product = vuln.Product
					AND dimP.ProductDesc = vuln.ProductDesc
					AND dimP.ProductCategory = vuln.ProductCategory
					)		
		END;										
											
		BEGIN			
				UPDATE Product							
				SET Product.Product = r.Product,
					Product.ProductDesc = r.ProductDesc,
					Product.ProductCategory = r.ProductCategory,
					UpdatedOn = GETDATE()
				FROM Product dimP								
				JOIN(								
					SELECT DISTINCT Product,[ProductDesc], ProductCategory						
					FROM STG_IMEATech vuln	
					) r 					
				ON r.Product = dimP.Product
		END;

--------------------------------------- DimPAC ----------------------------------------------------

	BEGIN			
				INSERT INTO PAC(PACCode,[UpdatedOn], CreatedOn)								
				(SELECT DISTINCT A.PACCode,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE PACCode IS NOT NULL								
				AND PACCode !=''	
				AND PACCode !='None'
				AND PACCode !='NA'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM PAC dimPAC	
					WHERE dimPAC.PACCode = A.PACCODE
					)	
				UNION
				SELECT DISTINCT PACCode,GETDATE(), GETDATE()	
				FROM STG_OO B
				WHERE PACCode IS NOT NULL								
				AND PACCode !=''	
				AND PACCode !='None'
				AND PACCode !='NA'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM PAC dimPAC	
					WHERE dimPAC.PACCode = B.PACCODE
					)	
				UNION
				SELECT DISTINCT PACCode,GETDATE(), GETDATE()			
				FROM STG_IMEATech C
					WHERE PACCode IS NOT NULL								
				AND PACCode !=''	
				AND PACCode !='None'
				AND PACCode !='NA'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM PAC dimPAC	
					WHERE dimPAC.PACCode = C.PACCODE
					)	
					)
		END;										

													
		BEGIN			
				UPDATE PAC							
				SET PAC.PACCode = r.PACCode,
					UpdatedOn = GETDATE()
				FROM PAC dimPAC								
				JOIN(								
					SELECT DISTINCT PACCode					
					FROM STG_NetActuals
					UNION
					SELECT DISTINCT A.PACCode	
					FROM STG_OO A 
					UNION
					SELECT DISTINCT A.PACCode		
					FROM STG_IMEATech  A
					) r 					
				ON r.PACCODE = dimPAC.PACCode
		END;
-------------------------------dimSalesStage---------------------------------------------------------------------------------
		BEGIN			
				INSERT INTO SalesStage([SalesStage], [UpdatedOn], CreatedOn)								
				SELECT DISTINCT [SalesStage], GETDATE(), GETDATE()							
				FROM STG_OO vuln								
				WHERE [SalesStage] IS NOT NULL								
				AND [SalesStage] !=''	
				AND [SalesStage] !='None'	
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM SalesStage dimSS	
					WHERE dimSS.SalesStage = vuln.SalesStage			
					)		
		END;										
													
		BEGIN			
				UPDATE SalesStage							
				SET salesstage.SalesStage = r.salesstage,
					UpdatedOn = GETDATE()
				FROM SalesStage dimSS							
				JOIN(								
					SELECT DISTINCT [SalesStage] 						
					FROM STG_OO vuln	
					) r 					
				ON r.SalesStage = dimSS.SalesStage
		END;		

----------------------------dimActualPlan-----------------------------------------------------------

	BEGIN			
				INSERT INTO ActualPlan ([ActualPlan], [UpdatedOn], CreatedOn)								
				SELECT DISTINCT [ActualsPlan], GETDATE(), GETDATE()							
				FROM STG_NetActuals vuln								
				WHERE [ActualsPlan] IS NOT NULL								
				AND [ActualsPlan] !=''	
				AND [ActualsPlan] !='None'	
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM ActualPlan dimA
					WHERE dimA.ActualPlan = vuln.ActualsPlan		
					)		
		END;										
												
		BEGIN			
				UPDATE ActualPlan						
				SET ActualPlan.ActualPlan = r.ActualsPlan,
					UpdatedOn = GETDATE()
				FROM ActualPlan dimA								
				JOIN(								
					SELECT DISTINCT ActualsPlan				
					FROM STG_NetActuals	
					) r 					
				ON r.ActualsPlan = dimA.ActualPlan
		END;	

----------------------------dimCompetitors-----------------------------------------------------------

	BEGIN			
				INSERT INTO Competitors ([Competitors], [UpdatedOn], CreatedOn)								
				SELECT DISTINCT [Competitor], GETDATE(), GETDATE()							
				FROM STG_NetActuals vuln								
				WHERE [Competitor] IS NOT NULL								
				AND [Competitor] !=''	
				AND [Competitor] !='None'	
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Competitors dimC
					WHERE dimC.Competitors = vuln.Competitor	
					)		
		END;										
												
		BEGIN			
				UPDATE Competitors					
				SET competitors.Competitors = r.Competitor,
					UpdatedOn = GETDATE()
				FROM Competitors dimC								
				JOIN(								
					SELECT DISTINCT Competitor			
					FROM STG_NetActuals	
					) r 					
				ON r.Competitor = dimC.Competitors
		END;

--------------------------------------- DimECOLOCATION ----------------------------------------------------

	BEGIN			
				INSERT INTO EcoLOCATION(City,[STATEPROVINCE],COUNTRY, COUNTRYCLUSTER,COUNTRYUPDATE,Region, CreatedOn, UpdatedOn)								
				(SELECT DISTINCT City,StateProvince,Country,CountryCluster,CountryUpdate,Region,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE City IS NOT NULL								
				AND City !=''	
				AND City !='None'
				AND Country IS NOT NULL								
				AND Country!=''	
				AND Country!='None'
				AND CountryCluster IS NOT NULL								
				AND CountryCluster!=''	
				AND CountryCluster!='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM EcoLOCATION dimE
					WHERE dimE.CITY = A.City
					and dimE.STATEPROVINCE = A.StateProvince
					and dimE.COUNTRY = A.Country
					and dimE.COUNTRYCLUSTER = A.CountryCluster
					)	
				UNION
				SELECT DISTINCT City,StateProvince,Country,CountryCluster,CountryUpdate,'',GETDATE(), GETDATE()	
				FROM STG_OO A
				WHERE City IS NOT NULL								
				AND City !=''	
				AND City !='None'
				AND Country IS NOT NULL								
				AND Country!=''	
				AND Country!='None'
				AND CountryCluster IS NOT NULL								
				AND CountryCluster!=''	
				AND CountryCluster!='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM EcoLOCATION dimE
					WHERE dimE.CITY = A.City
					and dimE.STATEPROVINCE = A.StateProvince
					and dimE.COUNTRY = A.Country
					and dimE.COUNTRYCLUSTER = A.CountryCluster
					and dimE.COUNTRYUPDATE = A.CountryUpdate
					)	
				UNION
				SELECT DISTINCT '','',Country,'',CountryUpdate,Region,GETDATE(), GETDATE()			
				FROM STG_IMEATech A
				where Country IS NOT NULL								
				AND Country!=''	
				AND Country!='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM EcoLOCATION dimE
					where dimE.COUNTRY = A.Country
					and dimE.COUNTRYUPDATE = A.CountryUpdate
					and dimE.REGION = A.Region
					)		
					)
		END;
		
		BEGIN			
				UPDATE EcoLOCATION							
				SET EcoLOCATION.CITY = r.City,
					EcoLocation.Country = r.COUNTRY,
					Ecolocation.CountryCluster = r.COUNTRYCLUSTER,
					UpdatedOn = GETDATE()
				FROM EcoLOCATION dimE							
				JOIN(								
					SELECT DISTINCT City,StateProvince,Country,CountryCluster,CountryUpdate,Region					
					FROM STG_NetActuals
					UNION
					SELECT DISTINCT City,StateProvince,Country,CountryCluster,CountryUpdate,''
					FROM STG_OO 
					UNION
					SELECT DISTINCT '','',Country,'',CountryUpdate,Region	
					FROM STG_IMEATech  
					) r					
				ON r.City = dimE.CITY
				AND r.Country = dimE.COUNTRY
				AND r.CountryCluster = dimE.COUNTRYCLUSTER
		END;

--------------------------------------- DimTechnologyPlatform ----------------------------------------------------

	BEGIN			
				INSERT INTO TechnologyPlatform (TechPlatform,DigitalEnablers,Innovation,Digital,CreatedOn, UpdatedOn)								
				(SELECT DISTINCT TechPlatform,DigitalEnablers,Innovation,Digital,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE TechPlatform IS NOT NULL								
				AND TechPlatform !=''	
				AND TechPlatform !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM TechnologyPlatform dimT
					WHERE dimT.TechPlatform = A.TechPlatform
					)	
				UNION
				SELECT DISTINCT TechPlatform,DigitalEnablers,Innovation,Digital,GETDATE(), GETDATE()			
				FROM STG_OO A  
				WHERE TechPlatform IS NOT NULL								
				AND TechPlatform !=''	
				AND TechPlatform !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM TechnologyPlatform dimT
					WHERE dimT.TechPlatform = A.TechPlatform
					)
				UNION
				SELECT DISTINCT TechPlatform,'','','',GETDATE(), GETDATE()			
				FROM STG_IMEATech A  
				WHERE TechPlatform IS NOT NULL								
				AND TechPlatform !=''	
				AND TechPlatform !='N/A'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM TechnologyPlatform dimT
					WHERE dimT.TechPlatform = A.TechPlatform
					)
					)
		END;
		
		BEGIN			
				UPDATE TechnologyPlatform							
				SET TechnologyPlatform.TechPlatform = r.TechPlatform,
					TechnologyPlatform.DigitalEnablers = r.DigitalEnablers,
					TechnologyPlatform.Innovation = r.Innovation,
					TechnologyPlatform.Digital = r.Digital,
					UpdatedOn = GETDATE()
				FROM TechnologyPlatform dimT						
				JOIN(								
				SELECT DISTINCT TechPlatform,DigitalEnablers,Innovation,Digital			
				FROM STG_NetActuals A  
				UNION
				SELECT DISTINCT TechPlatform,DigitalEnablers,Innovation,Digital			
				FROM STG_OO A  
				UNION
				SELECT DISTINCT TechPlatform,'','',''		
				FROM STG_IMEATech A  
					) r					
				ON r.TechPlatform = dimT.TechPlatform
		END;

--------------------------------------- DimCustAccount ----------------------------------------------------

	BEGIN			
				INSERT INTO CustAccount(AccountName,AccountNumber,CorporateAccount,AcctType,SHIPNAME, CreatedOn,UpdatedOn)								
				(SELECT DISTINCT AccountName,AccountNumber,CorporateAccount,AcctType,'',GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE AccountName IS NOT NULL								
				AND AccountName !=''	
				AND AccountName !='None'
				AND AccountNumber IS NOT NULL								
				AND AccountNumber !=''	
				AND AccountNumber !='None'
				AND AcctType IS NOT NULL
				AND AcctType !=''
				AND AcctType !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM CustAccount dimCA
					WHERE dimCA.AccountName = A.AccountName
					AND dimCA.AccountNumber = A.AccountNumber
					AND dimCA.AcctType = A.AcctType
					)	
				UNION
				SELECT DISTINCT AccountName,AccountNumber,CorporateAccount,AcctType,'',GETDATE(), GETDATE()			
				FROM STG_OO A  
				WHERE AccountName IS NOT NULL								
				AND AccountName !=''	
				AND AccountName !='None'
				AND AccountNumber IS NOT NULL								
				AND AccountNumber !=''	
				AND AccountNumber !='None'
				AND AcctType IS NOT NULL
				AND AcctType !=''
				AND AcctType !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM CustAccount dimCA
					WHERE dimCA.AccountName = A.AccountName
					AND dimCA.AccountNumber = A.AccountNumber
					AND dimCA.AcctType = A.AcctType
					)	
				UNION
				SELECT DISTINCT AccountName,AccountNumber,CorporateAccount,AcctType,ShipName,GETDATE(), GETDATE()			
				FROM STG_IMEATech A  
				WHERE AccountName IS NOT NULL								
				AND AccountName !=''	
				AND AccountName !='None'
				AND AccountNumber IS NOT NULL								
				AND AccountNumber !=''	
				AND AccountNumber !='None'
				AND AcctType IS NOT NULL
				AND AcctType !=''
				AND AcctType !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM CustAccount dimCA
					WHERE dimCA.AccountName = A.AccountName
					AND dimCA.AccountNumber = A.AccountNumber
					AND dimCA.AcctType = A.AcctType
					)	
					)
		END;
		
		BEGIN			
				UPDATE CustAccount							
				SET CustAccount.AccountName = r.AccountName,
					CustAccount.AccountNumber = r.AccountNumber,
					CustAccount.AcctType = r.AcctType,
					UpdatedOn = GETDATE()
				FROM CustAccount dimCA							
				JOIN(								
				SELECT DISTINCT AccountName,AccountNumber,CorporateAccount,AcctType			
				FROM STG_NetActuals A  
				UNION
				SELECT DISTINCT AccountName,AccountNumber,CorporateAccount,AcctType
				FROM STG_OO A  
				UNION
				SELECT DISTINCT AccountName,AccountNumber,CorporateAccount,AcctType			
				FROM STG_IMEATech A  
					) r					
				ON r.AccountName = dimCA.AccountName
				AND r.AccountNumber= dimCA.AccountNumber
		END;

--------------------------------------- DimSalesPerson ----------------------------------------------------

	BEGIN			
				INSERT INTO SalesPerson(TeamMember,AccountOwner,CorporateManager,CreatedOn, UpdatedOn)								
				(SELECT DISTINCT TeamMember,AccountOwner,CorporateManager,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				JOIN STG_IMEATech B ON A.AccountOwner = B.Salesman
				WHERE AccountOwner IS NOT NULL								
				AND AccountOwner !=''	
				AND AccountOwner !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM SalesPerson dimSP
					WHERE dimSP.AccountOwner = A.AccountOwner
					)
				UNION
				SELECT DISTINCT TeamMember,AccountOwner,CorporateManager,GETDATE(), GETDATE()			
				FROM STG_OO A  
				JOIN STG_IMEATech B ON A.AccountOwner = B.Salesman
				WHERE AccountOwner IS NOT NULL								
				AND AccountOwner !=''	
				AND AccountOwner !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM SalesPerson dimSP
					WHERE dimSP.AccountOwner = A.AccountOwner
					)
				UNION
				SELECT DISTINCT TeamMember,AccountOwner,CorporateManager,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				JOIN STG_IMEATech B ON A.AccountOwner = B.Salesman
				WHERE AccountOwner IS NOT NULL								
				AND AccountOwner !=''	
				AND AccountOwner !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM SalesPerson dimSP
					WHERE dimSP.AccountOwner = A.AccountOwner
					)	
					)
		END;
		
		BEGIN			
				UPDATE SalesPerson						
				SET SalesPerson.AccountOwner = r.AccountOwner,
					SalesPerson.TeamMember = r.TeamMember,
					UpdatedOn = GETDATE()
				FROM SalesPerson dimSP						
				JOIN(								
				SELECT DISTINCT TeamMember,AccountOwner,CorporateManager		
				FROM STG_NetActuals A  
				JOIN STG_IMEATech B ON A.AccountOwner = B.Salesman
				UNION
				SELECT DISTINCT TeamMember,AccountOwner,CorporateManager		
				FROM STG_OO A  
				JOIN STG_IMEATech B ON A.AccountOwner = B.Salesman
				UNION
				SELECT DISTINCT TeamMember,AccountOwner,CorporateManager		
				FROM STG_NetActuals A  
				JOIN STG_IMEATech B ON A.AccountOwner = B.Salesman
					) r					
				ON r.AccountOwner = dimSP.AccountOwner
		END;

--------------------------------------- DimOpportunity ----------------------------------------------------

	BEGIN			
				INSERT INTO Opportunity(OpportunityStatus,OpportunityName,OpportunityType,CreatedOn, UpdatedOn)								
				(SELECT DISTINCT OpportunityStatus,OpportunityName,OpportunityType,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE OpportunityName IS NOT NULL								
				AND OpportunityName !=''	
				AND OpportunityName !='None'
				and OpportunityType IS NOT NULL								
				AND OpportunityType !=''	
				AND OpportunityType !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Opportunity dimO
					WHERE dimO.OpportunityName = A.OpportunityName
					AND dimO.OpportunityType = A.OpportunityType
					
					)
				UNION
				SELECT DISTINCT '',OpportunityName,OpportunityType,GETDATE(), GETDATE()			
				FROM STG_OO A  
				WHERE OpportunityName IS NOT NULL								
				AND OpportunityName !=''	
				AND OpportunityName !='None'
				and OpportunityType IS NOT NULL								
				AND OpportunityType !=''	
				AND OpportunityType !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Opportunity dimO
					WHERE dimO.OpportunityName = A.OpportunityName
					AND dimO.OpportunityType = A.OpportunityType
					) )
		END;
		
		BEGIN			
				UPDATE Opportunity						
				SET Opportunity.OpportunityName = r.OpportunityName,
					Opportunity.OpportunityType = r.opportunitytype,
					UpdatedOn = GETDATE()
				FROM Opportunity dimO						
				JOIN(								
				SELECT DISTINCT OpportunityStatus,OpportunityName,OpportunityType	
				FROM STG_NetActuals A  
				UNION
				SELECT DISTINCT '',OpportunityName,OpportunityType	
				FROM STG_OO A  
					) r					
				ON r.OpportunityName = dimO.OpportunityName
				AND r.OpportunityType = dimO.OpportunityType
		END;


--------------------------------------- DimIndustry ----------------------------------------------------

	BEGIN			
				INSERT INTO Industry(Industry, IndustryCode, SBU,SINCDesc,CreatedOn, UpdatedOn)								
				(SELECT DISTINCT Industry, IndustryCode, SBU,'',GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE Industry IS NOT NULL								
				AND Industry !=''	
				AND Industry !='None'
				and IndustryCode IS NOT NULL								
				AND IndustryCode !=''	
				AND IndustryCode !='None'
				AND SBU != ''
				AND SBU !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Industry dimI
					WHERE dimI.Industry = A.Industry
					AND dimI.IndustryCode = A.IndustryCode
					AND dimI.SBU = A.SBU
					)
				UNION
				SELECT DISTINCT Industry, IndustryCode, SBU,'',GETDATE(), GETDATE()			
				FROM STG_OO A  
				WHERE Industry IS NOT NULL								
				AND Industry !=''	
				AND Industry !='None'
				and IndustryCode IS NOT NULL								
				AND IndustryCode !=''	
				AND IndustryCode !='None'
				AND SBU != ''
				AND SBU !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Industry dimI
					WHERE dimI.Industry = A.Industry
					AND dimI.IndustryCode = A.IndustryCode
					AND dimI.SBU = A.SBU
					)
				UNION
				SELECT DISTINCT '','','',SINCDesc ,GETDATE(), GETDATE()			
				FROM STG_IMEATech A  
				WHERE SINCDesc != ''
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM Industry dimI
					WHERE dimI.SINCDesc = A.SINCDesc
					))
		END;
		
		BEGIN			
				UPDATE Industry					
				SET Industry.Industry = r.Industry,
					Industry.IndustryCode = r.IndustryCode,
					Industry.SBU = r.SBU,
					Industry.SINCDesc = r.SINCDesc,
					UpdatedOn = GETDATE()
				FROM Industry dimI						
				JOIN(								
				SELECT DISTINCT Industry, IndustryCode, SBU,SINCDesc		
				FROM STG_NetActuals A  
				UNION
				SELECT DISTINCT Industry, IndustryCode, SBU,''		
				FROM STG_OO A  
				UNION
				SELECT DISTINCT '','','',SINCDesc 			
				FROM STG_IMEATech A  
					) r					
				ON r.Industry = dimI.Industry
				AND r.IndustryCode = dimI.IndustryCode
				AND r.SBU = dimI.SBU
		END;

		BEGIN			
				INSERT INTO EcoDate(ActualCloseDate, EstimatedCloseDate, ShipmentDate,Quarter, Period, Month, Year,CreatedOn, UpdatedOn)								
				(SELECT DISTINCT ActualCloseDate,'', '', '',Period, Month, Year,GETDATE(), GETDATE()			
				FROM STG_NetActuals A  
				WHERE ActualCloseDate IS NOT NULL								
				AND ActualCloseDate !=''	
				AND ActualCloseDate !='None'
				and Period IS NOT NULL								
				AND Period !=''	
				AND Period !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM EcoDate dimD
					WHERE dimD.ActualCloseDate = A.ActualCloseDate
					AND dimD.Period = A.Period
					)
				UNION
				SELECT DISTINCT '',EstimatedCloseDate,'','','','','',GETDATE(), GETDATE()			
				FROM STG_OO A  
				WHERE EstimatedCloseDate IS NOT NULL								
				AND EstimatedCloseDate !=''	
				AND EstimatedCloseDate !='None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM EcoDate dimD
					WHERE dimD.EstimatedCloseDate = A.EstimatedCloseDate
					)
				UNION
				SELECT DISTINCT '',ShipmentDate,'','',Quarter ,'','', GETDATE(), GETDATE()			
				FROM STG_IMEATech A  
				WHERE ShipmentDATE is not null
				AND ShipmentDate != ''
				AND ShipmentDate != 'None'
				AND 1=1								
				AND not exists( 								
					SELECT 1			
					FROM EcoDate dimD
					WHERE dimD.ShipmentDate = A.ShipmentDate
					))
		END;
		

---------------------------------------------- FactWaterQuality------------------------------------------------														
		

		BEGIN
				DELETE Fact WHERE CONVERT(DATE,CreatedOn)=CONVERT(DATE,GETDATE())

				INSERT dbo.Fact(ActualPlanID,
							CompetitorsID,
							District,
							TechnologyID,
							AccountID,
							SalesPersonID,
							OpportunityID,
							IndustryID,
							ProductID,
							PACCodeID,
							SalesStageID,
							OODaysID,
							ECLYearID,
							ActualRevenueUSD, 
							Plan2021,
							RevenueEstimateUSD, 
							SumOfRevKUSD,
							RevenueperGM,
							RevenueMillions,
							CreatedOn,
							UpdatedOn,
							ActualCloseDateID,
							EstimatedCloseDateID,
							DateID
							)
				(SELECT DISTINCT dimA.ActualPlanID, dimC.CompetitorsID, dimE.District, dimT.TechnologyID,
				dimCA.AccountID, dimSP.SalesPersonID,'',dimI.IndustryID,'',dimPAC.PACCodeID,'',
				'','', ActualRevenueUSD, Plan2021,'','','','', GETDATE() [CreatedOn], GETDATE() [UpdatedOn], 
				dimdate1.DateID,'',''
				FROM STG_NetActuals vuln
				LEFT JOIN ActualPlan dimA ON  vuln.ActualsPlan = dimA.ActualPlan
				LEFT JOIN Competitors dimC ON  vuln.Competitor = dimC.Competitors
				LEFT JOIN EcoLOCATION dimE ON   vuln.District = dimE.DISTRICT
				LEFT JOIN TechnologyPlatform dimT ON  vuln.TechPlatform = dimT.TechPlatform
				LEFT JOIN CustAccount dimCA ON   vuln.AccountNumber = dimCA.AccountNumber
				LEFT JOIN SalesPerson dimSP ON  vuln.AccountOwner = dimSP.AccountOwner
				LEFT JOIN Industry AS dimI ON vuln.Industry = dimI.Industry
				LEFT JOIN PAC AS dimPAC ON vuln.PACCode = dimPAC.PACCode
				LEFT JOIN EcoDate AS dimdate1 ON dimdate1.ActualCloseDate = CONVERT(DATE,vuln.ActualCloseDate)
				GROUP BY dimA.ActualPlanID, dimC.CompetitorsID, dimE.District, dimT.TechnologyID,
				dimCA.AccountID, dimSP.SalesPersonID,dimI.IndustryID,dimPAC.PACCodeID,dimdate1.DateID, ActualRevenueUSD, Plan2021
				UNION
				SELECT DISTINCT '', dimC.CompetitorsID, dimE.District, dimT.TechnologyID,
				'', dimSP.SalesPersonID,dimO.OpportunityID,dimI.IndustryID,'',dimPAC.PACCodeID,dimSS.SalesStageID,
				dimOO.OODaysID,'', '','',RevenueEstimateUSD,'','','', GETDATE() [CreatedOn], GETDATE() [UpdatedOn], '',
				dimdate2.DateID,''
				FROM STG_OO vuln
				LEFT JOIN Opportunity dimO ON  vuln.OpportunityName = dimO.OpportunityName
				LEFT JOIN Competitors dimC ON  vuln.Competitor = dimC.Competitors
				LEFT JOIN EcoLOCATION dimE ON   vuln.District = dimE.DISTRICT
				LEFT JOIN TechnologyPlatform dimT ON  vuln.TechPlatform = dimT.TechPlatform
				LEFT JOIN CustAccount dimCA ON   vuln.AccountNumber = dimCA.AccountNumber
				LEFT JOIN SalesStage AS dimSS ON vuln.SalesStage = dimSS.SalesStage
				LEFT JOIN OODays dimOO ON  vuln.OODays = dimOO.OODays
				LEFT JOIN SalesPerson dimSP ON  vuln.AccountOwner = dimSP.AccountOwner
				LEFT JOIN Industry AS dimI ON vuln.Industry = dimI.Industry
				LEFT JOIN PAC AS dimPAC ON vuln.PACCode = dimPAC.PACCode
				LEFT JOIN EcoDate AS dimdate2 ON dimdate2.EstimatedCloseDate = CONVERT(DATE,vuln.EstimatedCloseDate)
				GROUP BY  dimC.CompetitorsID, dimE.District, dimT.TechnologyID,
				dimCA.AccountID, dimSP.SalesPersonID,dimO.OpportunityID,dimI.IndustryID,dimPAC.PACCodeID,dimSS.SalesStageID,
				dimOO.OODaysID,dimdate2.DateID, RevenueEstimateUSD
				UNION
				SELECT DISTINCT '', '', dimE.District, dimT.TechnologyID,
				dimCA.AccountID, dimSP.SalesPersonID,'',dimI.IndustryID,dimP.ProductID,dimPAC.PACCodeID,'',
				'',dimECL.ECLYearID, '','','',SumOfRevKUSD,RevenueperGM,RevenueMillions, GETDATE() [CreatedOn], GETDATE() [UpdatedOn], 
				'','',dimdate3.DateID
				FROM STG_IMEATech vuln
				LEFT JOIN Product dimP ON  vuln.Product = dimP.Product
				LEFT JOIN EcoLOCATION dimE ON   vuln.District = dimE.DISTRICT
				LEFT JOIN TechnologyPlatform dimT ON  vuln.TechPlatform = dimT.TechPlatform
				LEFT JOIN CustAccount dimCA ON   vuln.AccountNumber = dimCA.AccountNumber
				LEFT JOIN ECLYearr AS dimECL ON vuln.ECLYear = dimECL.ECLYear
				LEFT JOIN SalesPerson dimSP ON  vuln.Salesman = dimSP.AccountOwner
				LEFT JOIN Industry AS dimI ON vuln.Industry = dimI.Industry
				LEFT JOIN PAC AS dimPAC ON vuln.PACCode = dimPAC.PACCode
				LEFT JOIN EcoDate AS dimdate3 ON dimdate3.ShipmentDate = CONVERT(DATE,vuln.ShipmentDate)
				GROUP BY  dimE.District, dimT.TechnologyID,dimCA.AccountID, dimSP.SalesPersonID,dimI.IndustryID,dimP.ProductID,
				dimPAC.PACCodeID,dimECL.ECLYearID,dimdate3.DateID,SumOfRevKUSD,RevenueperGM,RevenueMillions)

		END;
		
END
