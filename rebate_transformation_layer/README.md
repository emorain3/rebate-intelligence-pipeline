# Rebate Intelligence Transformation Layer (dbt Core)

Welcome to the data transformation and modeling engine of the Rebate Intelligence Pipeline. This subfolder houses the **dbt Core** architecture that processes raw, multi-partner rebate ingestions from the landing zone and converts them into high-fidelity, analytics-ready star schemas within Snowflake.

The core objective of this layer is to solve three major business problems: structural inconsistency across data providers, row duplication, and hidden partner anomalies (such as "Silent Shops").

---

## 🏗️ Data Architecture (The Medallion Framework)

This project leverages a structured Medallion Architecture to isolate concerns and guarantee data predictability at every stage of the lifecycle:

1. **Staging Layer (Bronze):** Focuses on strict data type-casting, column name normalization, and initial single-column validation. Models here are built directly on top of raw raw source schemas.
2. **Intermediate Layer (Silver):** Implements business logic. This layer builds composite `MD5` surrogate keys, handles advanced row-number window deduplication, and injects boolean flags to dynamically isolate "Silent Shop" anomalies.
3. **Marts Layer (Gold):** Exposes optimized star schema configurations designed for heavy analytical consumption. This includes dimensional conformance (`dim_partners`), transactional aggregations (`fact_rebate_payouts`), and high-level rollup tables (`mart_affiliate_summary`).

---

## 🚦 Data Quality & Testing Gates

To maintain institutional trust in downstream metrics, this transformation layer implements a rigorous testing framework executing across every deployment:

* **27 Structured Data Tests:** A combination of out-of-the-box generic tests (`unique`, `not_null`, `relationships`) and custom validation asserts to prevent data drift or orphan keys from reaching production.
* **Semantic Lineage Control:** Explicitly links downstream consumption points using dbt `exposures`. The final metrics are bound back to their source models, providing a complete root-cause audit trail from raw ingestion to the executive dashboard.

---

## 📈 Pipeline Milestones & Progress

- [x] **Phase 1: Ingestion & Environment** - Raw datasets successfully structured in Snowflake (`DEV_MCP_DB.PUBLIC`).
- [x] **Phase 2: dbt Core Initialization** - Project structure, profiles, and initial connection gates validated.
- [x] **Phase 3 (Part A): Staging Layer (Bronze)** - Type-casting, column normalizations, and initial single-column validation rules applied.
- [x] **Phase 4: Intermediate Layer (Silver)** - Composite MD5 surrogate keys built, row-number window deduplication implemented, and silent shop anomaly tracking flags mapped.
- [x] **Phase 5: Analytical Marts (Gold)** - Modeled business-facing dimension and fact tables optimized for analytical query performance and storage constraints.
- [x] **Phase 6: Downstream Exposure Mapping** - Configured semantic exposure lineage (`exposures.yml`) pointing directly to the live execution endpoint, completing the end-to-end lineage graph.

---

## 💻 Essential Local Execution Commands

To run, validate, and document this transformation layer locally, utilize the following core lifecycle workflows:

```bash
# Clean project build targets and fetch updated packages
dbt clean && dbt deps

# Run and compile all upstream and downstream models
dbt run

# Execute the 27-point data quality test suite
dbt test

# Compile the local catalog documentation and lineage graph
dbt docs generate