-- =========================================================================
-- Assert Anomaly Regression Baseline:
-- 
-- Original production dataset: 106 affiliate locations flagged as silent shops
-- Synthesized/anonymized dataset: ~75 affiliate locations flagged under the same rule
-- The difference reflects resampling during data synthesis
--
-- This test does NOT assert an exact count because the synthesized dataset
-- introduces natural variance. Instead it guards against complete detection collapse:
-- if zero exceptions are flagged, the detection rule itself has broken.
--
-- dbt singular tests pass when the query returns zero rows.
-- This query returns one row only when detection has completely failed.
-- =========================================================================


select 1
from DEV_MCP_DB.PUBLIC.mart_affiliate_summary
having sum(case when is_silent_shop = true then 1 else 0 end) = 0