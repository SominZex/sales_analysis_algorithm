# System Architecture
![architechture](https://github.com/user-attachments/assets/af0879f1-918d-441a-be7a-012135240e8c)


## Overview

The Sales Analysis Automation System is a **batch-oriented, report-driven analytics pipeline** designed to operate without manual intervention. It follows a classical ETL + reporting architecture and is optimized for **reliability, automation, and scheduled delivery** rather than interactive exploration.

The system runs on a Linux-based **Azure Virtual Machine** and is orchestrated using **cron jobs**.

---

## High-Level Architecture

1. **Source Systems**
   - Mainframe or transactional systems
   - Sales data accessed via API calls

2. **ETL Layer**
   - Extracts raw sales data
   - Transforms data into analytical structures
   - Loads processed data into a sandbox / analytics database

3. **Analytics Layer**
   - Computes KPIs and business metrics
   - Performs period-based comparisons (daily, weekly, monthly)
   - Applies business rules and growth calculations

4. **Reporting Layer**
   - Generates structured PDF reports
   - Applies formatting, summaries, and conditional highlights

5. **Distribution Layer**
   - Sends reports automatically via:
     - Email (SMTP)
     - WhatsApp (automation or API-based)

6. **Orchestration & Scheduling**
   - Cron jobs control execution frequency
   - No manual triggering required after setup

---

## Execution Flow

1. Cron triggers ETL scripts
2. Data is refreshed in the analytics database
3. Analysis scripts compute KPIs
4. PDF reports are generated
5. Reports are distributed to stakeholders
6. Logs are written for each step

---

## Design Principles

- **Automation-first**: Zero manual dependency after deployment
- **Batch processing**: Optimized for scheduled execution
- **Deterministic outputs**: Same inputs always produce same reports
- **Operational transparency**: Logs for every critical step
- **Environment isolation**: Virtual environments and explicit paths

---

## Non-Goals (Intentional)

- No interactive dashboards
- No real-time streaming
- No end-user UI
- No manual triggering by business users

This architecture is intentionally designed for **operational reporting**, not exploratory analytics.
