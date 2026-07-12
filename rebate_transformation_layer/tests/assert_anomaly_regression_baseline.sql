-- Custom Singular Test: Dynamic Data Provenance & Regression Audit
-- =========================================================================
-- PURPOSE:
-- The original enterprise production dataset surfaced exactly 106 "silent shop" 
-- anomalies. To maintain strict data privacy for public portfolio deployment, 
-- a resampled, synthesized dataset was generated. 
--
-- Running the exact same anomaly identification rules against this synthetic 
-- baseline yields a baseline of ~75 anomaly records. 
--
-- DATA CONTRACT OBJECTIVE:
-- This test acts as a regression boundary. It ensures the pipeline is actively 
-- capturing anomalies without hardcoding rigid expectations. It will only fail 
-- if a catastrophic upstream failure drops our anomaly tracking entirely to 0.
-- =========================================================================

with anomaly_snapshot as (
    select 
        count(distinct unique_key) as total_detected_anomalies
    from {{ ref('int_rebates') }}
    where is_exception = true
)

select 
    total_detected_anomalies
from anomaly_snapshot
-- Regressive safety gate: Fail only if data ingestion completely breaks and yields 0 anomalies
where total_detected_anomalies = 0