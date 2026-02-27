# Sales Analysis Automation System
# Flowchart
<img width="1920" height="1080" alt="flowchrt" src="https://github.com/user-attachments/assets/454e4bf6-a3bc-4a28-b916-6d92e24f8a59" />


## Overview
### This project is a fully automated sales reporting engine designed to:
- Generate Daily, Weekly, and Monthly business performance reports
- Process structured ETL workflows
- Automatically distribute reports via Email and WhatsApp
- Operate without manual intervention after deployment
### The system follows a hybrid orchestration architecture:
- Apache Airflow orchestrates ETL, reporting, and notifications


## Key Capabilities 
- End-to-end Airflow orchestration
- Deterministic DAG execution
- Daily, Weekly, Monthly branching logic
- Automated ETL pipeline
- Growth comparison logic
- Automated PDF generation
- WhatsApp automation
- Email automation
- Structured logging
- Built-in retry & alerting
- Zero manual operational dependency

## System Architecture
#### The system follows a layered orchestration strategy:

| Layer          | Technology                  | Responsibility                                     |
| -------------- | --------------------------- | -------------------------------------------------- |
| Orchestration     | Apache Airflow (Dockerized) | ETL, Analysis, Reporting, Notifications          |


#### Benefits:
- Centralized scheduling
- Clear dependency management
- Built-in retries & alerting
- Simplified operations
- No time-drift between systems

### Data Engineering Layer (Apache Airflow)
#### The ETL layer is orchestrated using Apache Airflow, deployed via Docker.
### Responsibility
- Sales data is fetched via API calls from the mainframe database
- Data is loaded into a sandbox / analytics database
- Orchestrates data extraction and transformation tasks
- Manages dependencies between ETL jobs
- Provides retry, monitoring, logging, and scheduling
- Ensures deterministic execution of structured data workflows

## Design Characteristics
- DAG-driven deterministic execution
- Time-window controlled processing
- Safe re-runs (idempotent writes)
- Dependency-aware scheduling
- Controlled retry policy
- Containerized execution environment

### Reporting & Distribution Layer (Cron-Based)
####  The reporting layer runs directly on the Azure VM using cron scheduling.

### Responsibilities
- PDF report generation
- Excel report creation
- WhatsApp automation
- Email distribution
- Duplicate prevention logic
- File-system bound execution


#### Running browser automation inside containerized Airflow workers introduced:
- Browser instability
- Resource contention
- Increased orchestration complexity

#### Therefore, delivery workloads are intentionally isolated at the OS level.
These workloads involve browser sessions and GUI-level automation, which are better handled in controlled shell execution environments rather than containerized orchestration.

## Execution Flow

```mermaid
flowchart TD
    A[Airflow Scheduler] --> B[ETL Pipeline]
    B --> C[Product Update]
    C --> D[Daily Analysis]
    D --> E[Daily WhatsApp]
    E --> F{Weekly?}
    F -->|Yes| G[Weekly Reports]
    G --> H[Weekly Mail]
    F -->|No| I[Skip Weekly]
    E --> J{Monthly?}
    J -->|Yes| K[Monthly Reports]
    K --> L[Monthly Mail]
    J -->|No| M[Skip Monthly]
```

## Reporting Logic
#### Daily Report
- Store-wise sales
- Category-wise sales
- Brand-wise sales
- Product-wise sales
- Comparison against rolling 7-day average
- Growth percentage computation
- Conditional formatting for performance signals
#### Weekly Report
- Aggregates prior week performance
- Designed for operational and management stakeholders

## Monthly Report
- Consolidated monthly business performance
- Strategic performance summary


## Installation & Setup
##### Clone the Repository:

```bash
git clone https://github.com/SominZex/sales_analysis_algorithm.git
```

```bash
cd sales_analysis_algorithm
```

## Create environment
```bash
python3 -m venv env_name
```

## Activate env
```bash
env_name/bin/activate
```

## Install Dependencies:
```bash
pip install -r requirements.txt
```

### Infrastructure & Deployment
#### Containerized ETL (Docker + Airflow)
Airflow runs in Docker for:
- Environment reproducibility
- Dependency isolation
- Controlled orchestration
- Clean separation from OS-level automation

## Airflow Setup:
### Directory Structure
```bash
/sales_analysis_algorithm/
    /airflow/
        ├── dags/
        ├── docker-compose.yml
```

#### Start
```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```
### Access UI
```bash
http://localhost:8080
```

## Observability & Reliability
#### Logging
#### Logs are generated for:
- ETL execution (Airflow logs)
- Report generation
- Email delivery
- WhatsApp automation

#### Logs support:
- Failure diagnosis
- Operational auditing
- Execution traceability

#### Determinism
- Idempotent data writes
- Controlled execution windows
- Duplicate prevention safeguards
- Explicit scheduling boundaries

#### Fault Containment
- ETL failures isolated from reporting layer
- Reporting failures do not corrupt data layer
- Layered orchestration prevents cascading impact


## CI/Automation
### GitHub Actions are configured for:
- Code validation
- Basic test execution
- Runtime automation is handled exclusively by cron on Azure VM

### Runtime scheduling is controlled by:
- Airflow (Data Layer)
- Cron (Delivery Layer)

## Design Principles
- Separation of concerns
- Deterministic data workflows
- Hybrid orchestration where appropriate
- Infrastructure-aware engineering decisions
- Production stability over tool overuse
- Minimal human operational dependency

## Final Note
#### This system is not a dashboard.
#### It is a production-grade, hybrid-orchestrated Sales Intelligence Automation Engine engineered for: 
- Reliability
- Determinism
- Operational scalability
- Controlled execution boundaries
- Continuous automated insight delivery

