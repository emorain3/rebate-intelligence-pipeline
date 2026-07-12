Current Pipeline Progress

- [x] **Phase 1: Ingestion & Environment** - Raw datasets successfully structured in Snowflake (`DEV_MCP_DB.PUBLIC`).
- [x] **Phase 2: dbt Core Initialization** - Project structure, profiles, and initial connection gates validated.
- [x] **Phase 3 (Part A): Staging Layer (Bronze)** - Type-casting, column normalizations, and initial single-column validation rules applied.
- [x] **Phase 3 (Part B): Intermediate Layer (Silver)** - Composite MD5 surrogate keys built, row-number window deduplication implemented, and silent shop anomaly tracking flags mapped.