SELECT SpeciesStock.Description AS Stock_Name, 
	   Year(CWDBRecovery.RecoveryDate) AS Recovery_Year, 
	   Month(CWDBRecovery.RecoveryDate) AS Recovery_Month, 
	   CFileFishery.Id AS CFileFishery_Id, 
	   CFileFishery.Description as CFileFishery_Name,
	   Age,
	   switch(left(CWTMark1, 1) = "5", "Adclipped", left(CWTMark1, 1) = "0", "Unclipped") as Release_Status,
	   Count(RecoveryId) as ObservedTotal,
	   Sum(CWDBRecovery.AdjustedEstimatedNumber) AS SumOfAdjustedEstimatedNumber,
	   Sum(CWDBRecovery.AdjustedEstimatedNumber *  
	        ((Nz(CWTMark1Count, 0) + Nz(CWTMark2Count, 0) + Nz(NonCWTMark1Count, 0) + Nz(NonCWTMark2Count, 0)) /
			 (Nz(CWTMark1Count, 0) + Nz(CWTMark2Count, 0)))) as Total_Expanded
FROM ((Fishery 
		INNER JOIN ((CWDBRecovery
		INNER JOIN WireTagCode ON CWDBRecovery.TagCode = WireTagCode.TagCode)
		INNER JOIN SpeciesStock ON WireTagCode.Stock = SpeciesStock.Stock) ON Fishery.Id = CWDBRecovery.Fishery) 
		INNER JOIN FisheryCFileFishery ON Fishery.Id = FisheryCFileFishery.Fishery) 
		INNER JOIN CFileFishery ON FisheryCFileFishery.CFileFishery = CFileFishery.Id
WHERE Year(CWDBRecovery.RecoveryDate)>= 2009
GROUP BY SpeciesStock.Description, 
		 Year(CWDBRecovery.RecoveryDate), 
		 Month(CWDBRecovery.RecoveryDate), 
		 Age,
		 CFileFishery.Id, 
		 CFileFishery.Description,
		 switch(left(CWTMark1, 1) = "5", "Adclipped", left(CWTMark1, 1) = "0", "Unclipped")
ORDER BY SpeciesStock.Description,
		 Year(CWDBRecovery.RecoveryDate), 
		 Month(CWDBRecovery.RecoveryDate), 
		 CFileFishery.Id, 
		 CFileFishery.Description,
		 Age,
		 switch(left(CWTMark1, 1) = "5", "Adclipped", left(CWTMark1, 1) = "0", "Unclipped");
