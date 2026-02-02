-- SQL CODE TO QUERY UNICORN 7.11 DATABASE
-- 20251120 ST 
-- Acknowledge Bramie, MingChen, Daniella Haught
WITH folderHierarchyData as 
	(
		SELECT f.FolderID, f.ParentFolderID, f.Description, f.IsGlobal, f.IsHidden, f.Created, f.CreatedBy, f.LastModified, f.LastModifiedBy, f.tstamp,	1 as Level, cast ( concat('/',f.Description) as varchar(max) ) as FolderPath
		FROM dbo.Folder f 
		WHERE f.ParentFolderID is null and f.IsHidden = 0
		UNION ALL
		SELECT m.FolderID, m.ParentFolderID, m.Description, m.IsGlobal, m.IsHidden, m.Created, m.CreatedBy, m.LastModified, m.LastModifiedBy, m.tstamp, r.Level+1 as Level,cast ( concat(r.FolderPath,'/',m.Description) as varchar(max) ) as FolderPath
		FROM dbo.Folder m
		INNER JOIN folderHierarchyData r
		ON m.ParentFolderID = r.FolderID
	),
	resultData as
	(
		select
		f.FolderPath,
		r.ResultID,
		r.Description as "ResultName",
		r.SystemName,
		s.ComputerName,
		s.ICUSerialNumber,
		ic.Description as "InstrumentDescription",
		ic.Version as "StrategyVersion",
		ic.StrategyName as "StrategyName",
		r.RunIndex,
		r.RunTypeName,
		r.Created as "ResultCreated",
		r.CreatedBy as "ResultCreatedBy",
		r.LastModified as "ResultLastModified",
		r.LastModifiedBy as "ResultLastModifiedBy",
		ch.ChromatogramID,
		ch.OriginalResultName,
		ch.Description as "ChromatogramDescription",
		ch.Created as "ChromatogramCreated",
		ch.TimeUnit as "ChromatogramTimeUnit",
		ch.VolumeUnit as "ChromatogramVolumeUnit"
		from dbo.[Result] r
		left join dbo.Chromatogram ch on r.ResultID = ch.ResultID
		left join dbo.UCSystem s on r.SystemName = s.SystemName
		left join dbo.InstrumentConfiguration ic on s.InstrumentConfigurationID = ic.InstrumentConfigurationID 
		left join folderHierarchyData f on r.FolderID = f.FolderID
	),
	curveData as 
	(
		select 
		c.ChromatogramPos,
		c.Description as "CurveDescription",
		c.IsoCroneTypeName,
		c.CurveDataTypeName,
		c.DistanceToStartPoint,
		c.DistanceBetweenPoints,
		c.ChromatogramStartTime,
		c.MethodStartTime,
		c.TimeUnit as "CurveTimeUnit",
		c.VolumeUnit as "CurveVolumeUnit",
		c.AmplitudePrecision,
		c.AmplitudeUnit,
		c.ColumnVolume,
		c.ColumnVolumeUnitName,
		cu.IsUVValuesNormalizedToNominalUVPathLength,
		cu.NominalUVPathLength,
		cu.UVPathLength,
		concat(c.Description,case c.AmplitudeUnit when '' then '' else concat(' (',c.AmplitudeUnit,')') end) as "CurveDescriptionWithUnit",
		r.*
		from dbo.Curve c
		left join dbo.CurveUVInfo cu on c.ChromatogramID = cu.ChromatogramID and c.ChromatogramPos = cu.ChromatogramPos 
		left join resultData r on c.ChromatogramID = r.ChromatogramID
	),
	curveBinaryData AS 
	(
		select 
		c.*,
		cp.IsFullResolution,
		cp.BinaryCurvePoints
		from dbo.CurvePoint cp
		left join curveData c on cp.ChromatogramID = c.ChromatogramID and cp.ChromatogramPos = c.ChromatogramPos 
	),
	eventData AS
	(
		select
		e.EventCurvePos,
		e.EventTypeName,
		e.EventSubTypeName,
		e.EventTime,
		e.EventVolume,
		e.EventText,
		e.EventText2,
		e.MarkValue,
		ec.ChromatogramPos as "EventChromatogramPos",
		ec.InjectionEventCurveChromID,
		ec.InjectionEventCurveChromPos,
		ec.EventCurveTypeName,
		ec.Description as "EventCurveDescription",
		ec.TimeUnit as "EventCurveTimeUnit",
		ec.VolumeUnit as "EventCurveVolumeUnit",
		r.*
		from dbo.EventCurve ec
		left join dbo.Event e on ec.ChromatogramID = e.ChromatogramID and ec.ChromatogramPos = e.ChromatogramPos 
		left join resultData r on r.ChromatogramID = ec.ChromatogramID 
	),
	logbookData as
	(
		SELECT
		*
		FROM eventData e
		where e.EventCurveTypeName = 'Logbook'
	),
	-- Matches pattern where Phases start and end sequentially.  Nested phases would require further development.
	lastEvent AS 
	(
		select
		l.ResultID,
		l.ChromatogramID,
		l.EventChromatogramPos,
		max(l.EventCurvePos) as EventCurveLastPos
		from logbookData l
		group by l.ResultID, l.ChromatogramID, l.EventChromatogramPos
	),
	phaseStart as
	(
		select
		l.ResultID, 
		l.ChromatogramID, 
		l.EventChromatogramPos, 
		l.EventCurvePos as PhaseStartPos,
		lead(l.EventCurvePos,1,le.EventCurveLastPos) over 
			(
			partition by l.ResultID, l.ChromatogramID, l.EventChromatogramPos
			order by l.EventCurvePos asc
			)
			as PhaseEndPos,
		replace(l.EventText,' (Issued) (Processing) (Completed)','') as PhaseName, 
		l.EventText as PhaseStartText, 
		l.EventTime as PhaseStartTime, 
		l.EventVolume as PhaseStartVolume, 
		l.EventCurveTimeUnit as PhaseTimeUnit, 
		l.EventCurveVolumeUnit as PhaseVolumeUnit
		from logbookData l
		left join lastEvent le on l.ResultID = le.ResultID and l.ChromatogramID = le.ChromatogramID and l.EventChromatogramPos = le.EventChromatogramPos 
		where (l.EventSubTypeName in ('BlockStart') and l.EventText like 'Phase%') or l.EventCurvePos = 1
	),
	phaseData as
	(
		select
		p.*,
		l.EventText as PhaseEndText,
		l.EventTime as PhaseEndTime,
		l.EventVolume as PhaseEndVolume
		from phaseStart p
		left join logbookData l on p.ResultID = l.ResultID and p.ChromatogramID = l.ChromatogramID and p.EventChromatogramPos = l.EventChromatogramPos and p.PhaseEndPos = l.EventCurvePos
	),
	fractionData as
	(
		SELECT
		*
		FROM eventData e
		where e.EventCurveTypeName = 'Fraction'
	),
	injectionData as
	(
		SELECT
		*
		FROM eventData e
		where e.EventCurveTypeName = 'Injection'
	),
	resultAuditData as
	(
		select
		rat.ResultID,
		rat.CalibrationSettingData,
		rat.ColumnTypeData,
		rat.ColumnIndividualData,
		rat.EvaluationProcedureData,
		rat.InstrumentConfigurationData,
		rat.InstrumentInformationData,
		rat.MethodData,
		rat.MethodDocumentationData,
		rat.NextBufferPrepData,
		rat.NextFracData,
		rat.ReportFormatData,
		rat.StrategyData,
		rat.SystemData,
		rat.SystemSettingData,
		rat.VersionInformationData
		from dbo.ResultAuditTrail rat
		left join resultData r on rat.ResultID = r.ResultID
	),
	resultVariablesData as
	(
		SELECT 
		rsc.ResultID,
		rsc.ResultSearchCriteriaID,
		rsc.SearchCriteriaName as KeyType,
		rsc.KeyWord1 as KeyWord,
		rsc.KeyWord2 as KeyValue,
		rsc.ExtraDisplayInformation as Units
		from dbo.ResultSearchCriteria rsc
		left join resultData r on rsc.ResultID = r.ResultID
	),
	peakData as 
	(
		SELECT
		pt.ChromatogramID as "IntegrationChromatogramID",
		pt.ChromatogramPos as "IntegrationChromatogramPos",
		pt.DataCurveChromID,
		pt.DataCurveChromPos,
		pt.BaseLineCurveChromID,
		pt.BaseLineCurveChromPos,
		pt.CalculationRetentionTypeName,
		pt.Description as "IntegrationDescription",
		pt.NumberOfDetectedPeaks,
		pt.TotalPeakArea,
		pt.TotalPeakAreaEvaluatedPeaks,
		pt.RatioPeakAreaTotalArea,
		pt.TotalPeakWidth,
		pt.ResolutionAlgorithmID,
		pt.AssymetryLevel,
		pt.MaxNumberOfPeaks,
		pt.MinPeakHeight,
		pt.MinPeakWidth,
		pt.MaxPeakWidth,
		pt.MinPeakArea,
		pt.Created as "IntegrationCreated",
		pt.ColumnHeight,
		pt.ColumnV0,
		pt.ColumnVt,
		pt.SkimRatio,
		pt.ColumnVolume as "IntegrationColumnVolume",
		pt.TechniqueName,
		pt.ZeroAdjustedToInjectionNumber,
		pt.OriginalResultName as "IntegrationOriginalResultName",
		pt.OriginalChromatogramName as "IntegrationOriginalChromatogramName", 
		pt.RecoveryFactor,
		pt.ComponentName,
		pt.BaselineType,
		pu.AreaUnit,
		pu.HeightUnit,
		pu.RetentionUnit,
		pu.ResolutionUnit,
		pu.CapacityFactorUnit,
		pu.PlateHeightUnit,
		pu.KavUnit,
		pu.AssymetryUnit,
		pu.ConductivityHeightUnit,
		pu.ColumnVolumeUnit,
		pu.ColumnHeightUnit,
		pu.ConcUnit,
		pu.AmountUnit,
		pu.MolSizeUnit,
		pu.RecAreaUnit,
		pu.RecVolumeUnit,
		pu.RecHeightUnit,
		pu.RecConcUnit,
		pu.ExtCoeffUnit,
		pu.ExtCoeffConcUnit,
		pu.ExtCoeffAmountUnit,
		pu.SmallHUnit,
		p.PeakTablePos,
		p.StartPeakLimitTypeName,
		p.EndPeakLimitTypeName,
		p.Description as "PeakDescription",
		p.Width,
		p.Area,
		p.Height,
		p.StartPeakRetention,
		p.MaxPeakRetention,
		p.EndPeakRetention,
		p.WidthAtHalfHeight,
		p.PercentOfTotalArea,
		p.PercentOfTotalPeakArea,
		p.StartPeakEndPointHeight,
		p.EndPeakEndPointHeight,
		p.StartBaseLineHeight,
		p.MaxBaseLineHeight,
		p.EndBaseLineHeight,
		p.StartPeakVial,
		p.MaxPeakVial,
		p.EndPeakVial,
		p.Sigma,
		p.Resolution,
		p.CapacityFactor,
		p.Kav,
		p.PlateHeight,
		p.PlatesPerMeter,
		p.Assymetry,
		p.AssymetryPeakStart,
		p.AssymetryPeakEnd,
		p.StartConductivityHeight ,
		p.MaxConductivityHeight,
		p.EndConductivityHeight,
		p.SkimLineK,
		p.SkimLineM,
		p.VisualAppearance,
		p.Conc,
		p.Amount,
		p.MolSize,
		p.RecArea,
		p.RecVolume,
		p.RecHeight,
		p.RecConc,
		p.IsStandardPeak,
		p.ExtCoeff,
		p.ExtCoeffConc,
		p.ExtCoeffAmount,
		p.AverageConductivity,
		p.AveragePh,
		p.ReducedPlateHeight,
		p.SmallH,
		c.*
		from dbo.PeakTable pt
		left join dbo.PeakUnit pu on pt.ChromatogramID = pu.ChromatogramID and pt.ChromatogramPos = pu.ChromatogramPos 
		left join dbo.Peak p on pt.ChromatogramID = p.ChromatogramID and pt.ChromatogramPos = p.ChromatogramPos  
		left join curveData c on pt.DataCurveChromID = c.ChromatogramID and pt.DataCurveChromPos = c.ChromatogramPos 
	),
	resultCount AS (
		SELECT 
		r.FolderID,
		count(*) as 'Number of Results'
		FROM dbo.[Result] r
		GROUP BY r.FolderID
	),
	methodCount AS (
		select 
		m.FolderID,
		count(*) as 'Number of Methods'
		from dbo.[Method] m
		group by m.FolderID
	)
select *
FROM methodCount m 

-- ucaccess, pool, pooltable
