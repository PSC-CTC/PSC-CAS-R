SELECT CWDBRecovery.RecoveryId, 
       CWDBRecovery.Agency, 
	   CWDBRecovery.RunYear, 
	   CWDBRecovery.RecoverySite, 
	   CWDBRecovery.RecoveryDate, 
	   CWDBRecovery.Species, 
	   CWDBRecovery.CWDBFishery, 
	   CWDBRecovery.Fishery as FisheryID, 
	   Fishery.Description as FisheryName, 
	   CWDBRecovery.Sex, 
	   CWDBRecovery.Length, 
	   CWDBRecovery.LengthCode, 
	   CWDBRecovery.Age, 
	   CWDBRecovery.TagStatus, 
	   CWDBRecovery.TagCode, 
	   CWDBRecovery.DetectionMethod, 
	   CWDBRecovery.RecordedMark, 
	   CWDBRecovery.SampleType, 
	   CWDBRecovery.SamplingPeriodType, 
	   CWDBRecovery.SamplingPeriodNumber, 
	   CWDBRecovery.EstimatedNumber, 
	   CWDBRecovery.AdjustedEstimatedNumber, 
	   CWDBRecovery.EstimationLevel, 
	   CWDBRecovery.CatchSampleId, 
	   CWDBRecovery.Extrapolated, 
	   CWDBRecovery.ExtrapolatedRecoveryId, 
	   CWDBRecovery.Auxiliary
FROM Fishery INNER JOIN CWDBRecovery ON Fishery.Id = CWDBRecovery.Fishery;
