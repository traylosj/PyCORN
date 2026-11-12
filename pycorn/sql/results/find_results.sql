-- =============================================================================
-- Run catalog: one row per result, one page at a time
-- =============================================================================
-- Used by pycorn.search (PyCORNDatabase.find / find_pages / count). Not run on its own:
-- pycorn.search replaces the markers (block comments with these names) before running it:
--   CTES     WITH ... for the folder and hidden-folder filters (or nothing)
--   SELECT   the catalog columns (find), or COUNT(*) (count)
--   TOP      TOP (page size + 1), so the caller knows if there are more
--   FILTERS  AND ... clauses; every value is a pymssql parameter
--   ORDER    ORDER BY r.ResultID (find only; paging continues after the last ResultID)
-- Don't write the markers or percent signs in comments: they would be replaced or read
-- as parameters.
--
-- Run start: ResultRunInformation 'RunStartDate', stored as 'MM/DD/YYYY hh:mm:ss +HH:MM'
-- (tested against a lab environment). Text in any other shape falls back to
-- the first Chromatogram.Created. No TRY_CONVERT: UNICORN databases can run at
-- compatibility level 100, so the LIKE guard keeps CONVERT from failing.
-- Column: the name in the run-time 'ColumnInformation' record (usedColumns), as stored
-- ('Any' = no column type chosen). XML entities are decoded by pycorn.search.
--
-- Other UNICORN versions may lack some columns. Blocks written as IF Table.Column ...
-- (block comment), then ELSE, then END (block comments) keep the first part when the
-- database has every listed column, else the part after ELSE (an empty value), so the
-- query still runs. pycorn.search resolves them from the database's SchemaProfile.
-- =============================================================================
/*CTES*/
SELECT /*TOP*/ /*SELECT*/
FROM dbo.[Result] r
/*IF ResultRunInformation.ResultID ResultRunInformation.RunInformationTypeName ResultRunInformation.RunInformation*/
OUTER APPLY (
    SELECT TOP 1 ri.RunInformation AS stored
    FROM dbo.ResultRunInformation ri
    WHERE ri.ResultID = r.ResultID AND ri.RunInformationTypeName = 'RunStartDate'
        AND ri.RunInformation LIKE '[01][0-9]/[0-3][0-9]/[12][0-9][0-9][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [+-][01][0-9]:[0-5][0-9]'
) rsd
/*ELSE*/
OUTER APPLY (SELECT CAST(NULL AS nvarchar(max)) AS stored) rsd
/*END*/
/*IF Chromatogram.ResultID*/
OUTER APPLY (
    SELECT /*IF Chromatogram.Created*/MIN(ch.Created)/*ELSE*/CAST(NULL AS datetimeoffset)/*END*/ AS first_created,
           COUNT(*) AS n
    FROM dbo.Chromatogram ch
    WHERE ch.ResultID = r.ResultID
) chs
/*ELSE*/
OUTER APPLY (SELECT CAST(NULL AS datetimeoffset) AS first_created, CAST(NULL AS int) AS n) chs
/*END*/
CROSS APPLY (
    SELECT CASE WHEN rsd.stored IS NOT NULL
                THEN CONVERT(datetimeoffset, SUBSTRING(rsd.stored, 7, 4) + '-' + LEFT(rsd.stored, 2) + '-'
                     + SUBSTRING(rsd.stored, 4, 2) + 'T' + SUBSTRING(rsd.stored, 12, 8) + SUBSTRING(rsd.stored, 21, 6), 127)
                ELSE chs.first_created END AS run_start,
           CASE WHEN rsd.stored IS NOT NULL THEN 'RunStartDate'
                WHEN chs.first_created IS NOT NULL THEN 'Chromatogram.Created' END AS run_start_source
) rs
/*IF ResultRunInformation.ResultID ResultRunInformation.RunInformationTypeName ResultRunInformation.RunInformation*/
OUTER APPLY (
    SELECT TOP 1 ri.RunInformation AS stored
    FROM dbo.ResultRunInformation ri
    WHERE ri.ResultID = r.ResultID AND ri.RunInformationTypeName = 'ColumnInformation'
) colinfo
/*ELSE*/
OUTER APPLY (SELECT CAST(NULL AS nvarchar(max)) AS stored) colinfo
/*END*/
WHERE 1 = 1 /*FILTERS*/
/*ORDER*/
