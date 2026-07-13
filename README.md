# Rebate Intelligence Pipeline

An end-to-end data engineering portfolio project demonstrating production-grade pipeline design, anomaly detection, and exception monitoring across two implementation paradigms.

**[View the full case study →](https://emorain3.github.io/rebate-intelligence-pipeline-casestudy/)**

> **Privacy Note:** All data in this repository is fully synthesized. The original dataset was anonymized and resampled to protect the privacy of the source organization and any identifying business information. Findings such as silent shop counts reflect the resampled data and may differ from figures referenced in the original case study presentation.

**[View the dbt Lineage Graph →](https://emorain3.github.io/rebate-intelligence-pipeline/)**

---

## What This Project Demonstrates

- Medallion Architecture (Bronze / Silver / Gold) implemented across two technology paradigms
- Composite key deduplication and row-level exception classification in dbt
- Silent shop detection encoded as a testable dbt data contract with a custom singular test
- Architectural decision-making: when to choose Microsoft Fabric vs. Snowflake + dbt

---

## Stack

| Phase | Tools |
|---|---|
| Phase 1 — Original Pipeline | Python · Pandas · Microsoft Fabric · Power BI |
| Phase 2 — Modern Stack Rebuild | Snowflake · dbt Core · Streamlit *(in progress)* |

---

## Pipeline Progress

**Phase 1: Python + Power BI (Complete)**
- [x] Raw dataset ingestion and profiling
- [x] Bronze → Silver → Gold transformation in Python/Pandas
- [x] Composite key deduplication (transaction_id + partner_id + memo + net_amount)
- [x] Silent shop detection via anti-join
- [x] Centralized exception table with severity classification
- [x] Power BI dashboard — Executive Summary, Silent Shops, Anomaly Detail pages

**Phase 2: Snowflake + dbt (In Progress)**
- [x] Phase 1: Raw data loaded into Snowflake (`DEV_MCP_DB.PUBLIC`) via stage + COPY INTO
- [x] Phase 2: dbt project initialized — profiles, sources, and connection verified
- [x] Phase 3A: Staging layer — type casting, column normalization, source tests
- [x] Phase 3B: Intermediate layer — MD5 surrogate key, window deduplication, exception typing
- [x] Phase 3C: Marts layer — `dim_partners`, `fact_rebate_payouts`, `mart_affiliate_summary`
- [x] 25 passing dbt tests including referential integrity and custom singular regression baseline
- [x] Phase 4: dbt docs + lineage graph hosted on GitHub Pages — **[View Lineage Graph →](https://emorain3.github.io/rebate-intelligence-pipeline/)**
- [ ] Phase 5: Streamlit deployment 

---

## Repository Structure

```
rebate-intelligence-pipeline/
├── rebate_transformation_layer/         # Phase 2: dbt Core project
│   ├── models/
│   │   ├── staging/              # Bronze — type casting, normalization
│   │   ├── intermediate/         # Silver — deduplication, exception classification
│   │   └── marts/                # Gold — dim_partners, fact_rebate_payouts, mart_affiliate_summary
│   └── tests/
│       └── assert_anomaly_regression_baseline.sql
├── ./                     # Phase 1: Python pipeline scripts
└── README.md
```

---

## Key Engineering Decisions

**Composite Key over Single-Column Primary Key**
Transaction IDs are not unique — a single transaction can generate multiple rebate entries through rebate decomposition. The composite key `(transaction_id + partner_id + memo + net_amount)` treats each rebate event as its own grain, preventing valid revenue from being silently removed.

**Exception Typing over Binary Flagging**
Rather than a simple is_error boolean, each anomalous row is classified by exception type (`zero_amount`, `null_amount`, `negative_amount`, `null_memo`, `null_partner`). This separates data quality issues from inactivity signals — a missing memo is not the same business problem as a zero-dollar transaction.

**Silent Shop Classification at the Aggregate Layer**
Silent shop status is assigned at the Gold mart level, not the row level. An affiliate is only classified as a silent shop when their entire transaction history contains zero clean, valid financial activity. This prevents cosmetic data quality flags from being misread as inactivity.

**Fabric vs. Snowflake + dbt**
The original pipeline used Microsoft Fabric — appropriate for a BI-first, small-team environment with Power BI as the single output. The Phase 2 rebuild uses Snowflake + dbt to demonstrate the composable, data-platform-first pattern appropriate for multi-team environments where transformation logic requires version control, testing, and lineage as first-class engineering concerns.

---

## Case Study & Findings

Full findings — including silent shop analysis, exception classification design, and commercial impact framing — are documented on the case study site:

📄 **[emorain3.github.io/rebate-intelligence-pipeline-casestudy](https://emorain3.github.io/rebate-intelligence-pipeline-casestudy/)**

---

## Contact

**Ecclesia Morain** — Cloud Data Engineer

[LinkedIn](https://linkedin.com/in/ecclesiamorain) · [hire.ecclesia@outlook.com](mailto:hire.ecclesia@outlook.com)