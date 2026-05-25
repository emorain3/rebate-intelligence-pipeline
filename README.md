# Enterprise Collision Network (ECN) — Rebate Intelligence & Data Quality Monitoring Case Study

## Overview

This project is a business-focused **data engineering and analytics case study** centered around modernizing rebate monitoring operations for a fictional organization: **Enterprise Collision Network (ECN)**.

The solution was originally designed as a proof-of-concept architecture for a collision repair network operating within a distributor-rebate ecosystem. The objective was to demonstrate my solution-mindedness, systems thinking, and technical ability through a real-world operational problem.

I identified the need for this data infrastructure as a direct improvement of:

* operational trust
* rebate visibility
* anomaly detection
* reporting accuracy
* scalability
* business intelligence enablement ← (*this was a major operational gap for this team without a dedicated data engineering function*)

The project demonstrates how modern data engineering practices can help organizations move away from ***reactive*** spreadsheet-driven operations toward ***proactive***, analytics-driven decision making.

---

# Business Problem

ECN relied heavily on manual rebate reconciliation processes across distributor-submitted transaction files.

The core challenge was “bad data, but even moreso the challenge was to mitigate the erosion of trust between:

* affiliates
* distributors
* operations teams
* leadership stakeholders

Manual workflows made it difficult to:

* identify missing expected activity
* detect reporting inconsistencies
* validate rebate integrity
* surface operational risks quickly
* scale business intelligence efforts effectively

This project was designed to demonstrate how automated monitoring, layered architecture, and exception-driven reporting could both (a) help reduce those risks, and (b) improve operational visibility.

---

# Quick Project Snapshot

| Category      | Details                                                      |
| ------------- | ------------------------------------------------------------ |
| Project Type  | Data Engineering / BI Case Study                             |
| Architecture  | Bronze → Silver → Gold Medallion                             |
| Primary Tools | Python, Pandas, Power BI                                     |
| Focus Areas   | ETL, anomaly detection, operational reporting, BI enablement |
| Business Goal | Improve rebate monitoring and reduce operational blind spots |
| Dataset       | Fully synthetic / anonymized                                 |
| Status        | Portfolio Case Study                                         |

---

# Key Capabilities Demonstrated

* Data Engineering
* ETL Pipeline Design
* Medallion Architecture (Bronze / Silver / Gold)
* Data Quality Monitoring
* Exception Management
* Anomaly Detection Concepts
* Power BI Reporting
* Business Intelligence Enablement
* Data Governance Planning
* Microsoft Fabric Architecture Planning
* Business-to-Technical Translation
* Stakeholder-Oriented Solution Design

---

# Synthetic Data Notice

All datasets included in this repository have been **synthetically regenerated** using Python-based fake data generation and custom anomaly simulation logic.

No real company data, affiliate identities, financial records, or proprietary business information are included in this repository.

The original case-study dataset was anonymized and replaced specifically to preserve confidentiality and eliminate exposure of any sensitive operational information.

---

# Solution Architecture

The project follows a simplified **Medallion Architecture** approach:

```text
Distributor Files
        ↓
     Bronze
   (Raw Intake)
        ↓
     Silver
 (Validation & Standardization)
        ↓
      Gold
 (Business Monitoring Outputs)
        ↓
    Power BI
        ↓
 Operations & Leadership
```

---

## Bronze Layer - ELT in action.

Raw source files are preserved exactly as received._If anything fails further down the pipeline - needed data gets overwritten, business logic changes, technical failurewe we have the originsl data here as a reference. No need to request from the distibutor and potential encounter delays, or — again — that eroded trust_

This enables our team to:

* maintain auditability 
* preserve recovery points
* retain original distributor submissions

Characteristics:

* immutable raw ingestion
* no transformations, just the columnar addition of a ...
* timestamped intake logging

---

## Silver Layer

The Silver layer standardizes and validates incoming data.

Core transformations include:

* datatype normalization
* column standardization
* null handling
* duplicate handling
* parent-child relationship resolution
* data quality validation
* exception logging
* rebate monitoring logic

This layer acts as the primary operational quality-control checkpoint. From here any "bad data" (as established by the business rules and logic) gets flagged and separated from the good data to avoid any interruption in data being 

---

## Gold Layer

The Gold layer produces analytics-ready outputs for Power BI and operational review workflows.

Outputs include:

* monthly affiliate summaries
* missing activity detection
* rebate decline alerts
* partner feed health metrics
* KPI summaries
* reconciliation concepts
* exception narratives
* business monitoring tables

The Gold layer was designed specifically to help operations and BI teams prioritize investigation and action instead of manually searching through raw transaction files.

---

# Example Monitoring Scenarios

The monitoring framework was designed to identify:

* silent affiliate locations with missing expected activity
* abnormal rebate declines
* duplicate transaction anomalies
* negative-value rebate events
* distributor feed inconsistencies
* parent-child hierarchy mismatches
* reconciliation gaps
* sponsor compliance concerns

Each anomaly category was intentionally tied back to potential business outcomes, including:

* revenue leakage
* missed rebates
* operational inefficiency
* delayed reporting
* compliance concerns
* reduced partner trust

---

# Project Structure

## `/scripts/bronze_to_silver.py`

Transforms raw ingestion data into validated Silver-layer datasets.

---

## `/scripts/silver_to_gold.py`

Creates Gold-layer business outputs and monitoring datasets.

---

## `/data/`

Contains example Bronze, Silver, and Gold datasets.

---

## `/power_bi/`

Contains Power BI reporting assets and dashboard concepts.

---

# Power BI & Reporting Strategy

Power BI was used as the primary operational reporting and monitoring layer.

The reporting strategy emphasized:

* exception-driven workflows
* operational visibility
* affiliate health monitoring
* executive-friendly KPI communication
* actionable investigation queues

The objective was not simply to create dashboards.

The objective was to reduce the amount of time spent manually searching through raw transaction data and instead surface the records most likely to require business attention.

---

# Why Medallion Architecture Was Chosen

The Bronze → Silver → Gold structure was selected because it provides:

* auditability
* layered validation
* recovery capability
* scalability
* cleaner BI consumption
* separation of concerns
* resilience to schema evolution

This structure also supports future operational maturity without redesigning the entire pipeline.

Potential future enhancements include:

* automated orchestration
* API-based ingestion
* advanced anomaly detection
* Microsoft Fabric migration
* historical trend forecasting
* automated alert routing
* reconciliation automation

---

# Important Note on Prototype Scope

This repository represents a **rapid-development case study**, not production-hardened enterprise software.

The emphasis was placed on:

1. understanding the business model
2. identifying operational risk
3. designing scalable architecture
4. building monitoring concepts
5. demonstrating business-oriented engineering thinking

rather than polishing every implementation detail for enterprise deployment.

Additional engineering work would still be required for:

* CI/CD
* orchestration
* automated testing
* infrastructure hardening
* advanced security controls
* parameterization
* production monitoring
* performance optimization

---

# AI-Assisted Development Disclosure

AI-assisted development tools were used selectively to accelerate portions of the prototype ETL implementation under tight delivery timelines.

Architectural decisions, business logic design, anomaly detection concepts, data modeling, reporting strategy, and operational solution framing were independently designed and validated throughout the case-study process.

---

# How to Run the Proof of Concept

## Install Requirements

```bash
pip install -r requirements.txt
```

Primary dependencies include:

* pandas
* numpy
* openpyxl
* pyarrow

---

## Source Data Location

```bash
data/bronze/*.csv
```

---

## Run Bronze → Silver

```bash
python scripts/bronze_to_silver.py
```

---

## Run Silver → Gold

```bash
python scripts/silver_to_gold.py
```

---

## Review Outputs

```bash
data/silver/
data/gold/
```

Optional CSV exports may also be included for easier inspection in Excel or Power BI.

---

# Additional Case Study Materials

Additional architecture walkthroughs, diagrams, dashboard screenshots, and operational analysis materials will be published through the accompanying public case study page.

---

# Final Statement

This project was built to demonstrate the ability to:

* rapidly understand an unfamiliar business domain
* structure ambiguity into actionable systems
* connect technical implementation to business outcomes
* design scalable data workflows
* communicate effectively with both technical and non-technical stakeholders
* and approach data engineering as an operational enablement function rather than simply a coding exercise.

The broader goal of the project is to build systems that help organizations **trust, understand, and act on their data more effectively.**
