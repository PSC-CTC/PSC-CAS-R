SELECT SpeciesStock.Description AS Stock_Name, 
       WireTagCode.TagCode, 
	   Year(CWDBRecovery.RecoveryDate) AS Recovery_Year, 
	   Month(CWDBRecovery.RecoveryDate) AS Recovery_Month, 
	   CFileFishery.Id AS CFileFishery_Id, 
	   CFileFishery.Description as CFileFishery_Name, 
	   Sum(CWDBRecovery.AdjustedEstimatedNumber) AS SumOfAdjustedEstimatedNumber
FROM ((Fishery 
		INNER JOIN ((CWDBRecovery
		INNER JOIN WireTagCode ON CWDBRecovery.TagCode = WireTagCode.TagCode)
		INNER JOIN SpeciesStock ON WireTagCode.Stock = SpeciesStock.Stock) ON Fishery.Id = CWDBRecovery.Fishery) 
		INNER JOIN FisheryCFileFishery ON Fishery.Id = FisheryCFileFishery.Fishery) 
		INNER JOIN CFileFishery ON FisheryCFileFishery.CFileFishery = CFileFishery.Id
WHERE Year(CWDBRecovery.RecoveryDate)>= 2009
GROUP BY SpeciesStock.Description, 
	     WireTagCode.TagCode, 
		 Year(CWDBRecovery.RecoveryDate), 
		 Month(CWDBRecovery.RecoveryDate), 
		 CFileFishery.Id, 
		 CFileFishery.Description
ORDER BY SpeciesStock.Description, 
         WireTagCode.TagCode, 
		 Year(CWDBRecovery.RecoveryDate), 
		 Month(CWDBRecovery.RecoveryDate), 
		 CFileFishery.Id, 
		 CFileFishery.Description;
