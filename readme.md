# LLM-Integrated Sales Intelligence Automation Engine

A production-grade automated analytics platform that ingests retail sales data,
computes deterministic business intelligence signals, and generates
LLM-grounded operational recommendations delivered via automated reports.

# System Architecture

```mermaid
flowchart TD

A[Retail Sales API]

subgraph Data Engineering
B[Apache Airflow Scheduler]
C[ETL Pipeline]
D[PostgreSQL Analytics Store]
end

subgraph Intelligence Engine
E[KPI Computation]
F[Trend Detection]
G[Risk Scoring]
H[Structured Insights]
end

subgraph LLM Layer
I[Groq LLaMA 3.1]
J[Ollama Fallback]
K[Operational Recommendations]
end

subgraph Reporting
L[Report Generator]
M[Daily Report]
N[Weekly Report]
O[Monthly Report]
end

subgraph Distribution
P[Email Delivery]
Q[WhatsApp Automation]
end

subgraph Observability Layer
R[Prometheus]
S[Pushgateway]
T[Node Exporter]
U[Grafana Dashboards]
end

A --> B
B --> C
C --> D

D --> E
E --> F
F --> G
G --> H

H --> I
H --> J
I --> K
J --> K

K --> L

L --> M
L --> N
L --> O

M --> P
M --> Q
N --> P
O --> P

%% Observability flows
B --> S
C --> S
E --> S
L --> S
P --> S
Q --> S

S --> R
T --> R
R --> U ```

## Overview

### This project is a fully automated sales reporting engine designed to:
- Generate Daily, Weekly, and Monthly business performance reports
- Process structured ETL workflows
- Persist historical performance snapshots
- Detect performance risks and trend shifts
- Generate LLM-grounded action recommendations
- Automatically distribute reports via Email and WhatsApp
- Operate without manual intervention after deployment

### The system follows a hybrid orchestration architecture:
- Apache Airflow orchestrates ETL, reporting, and notifications
- A deterministic Intelligence Engine computes KPIs and risk signals
- A constrained LLM layer converts structured analytics into prioritized store-level actions


## Key Capabilities 
- End-to-end Airflow orchestration
- Deterministic DAG execution
- Daily, Weekly, Monthly branching logic
- Automated ETL pipeline
- Growth comparison logic (WoW & MoM)
- Snapshot-based historical memory
- Revenue-weighted risk scoring
- Predictive signal detection (stockout, margin erosion, rising stars)
- Automated PDF generation
- WhatsApp automation
- Email automation
- Structured logging
- Built-in retry & alerting
- Groq → Ollama fallback for LLM reliability
- Zero manual operational dependency

## System Architecture
#### The system follows a layered orchestration strategy:

| Layer               | Technology              | Responsibility            |
| ------------------- | ----------------------- | ------------------------- |
| Orchestration       | Apache Airflow (Docker) | Scheduling, DAG execution |
| Data Layer          | PostgreSQL              | Analytics storage         |
| Intelligence Engine | Python (Pandas + logic) | KPI, trends, risk scoring |
| LLM Layer           | Groq + Ollama           | Recommendation generation |
| Reporting           | Python                  | PDF/Excel generation      |
| Distribution        | SMTP + WhatsApp APIs    | Delivery                  |
| Observability       | Prometheus + Grafana    | Monitoring & metrics      |



#### Benefits:
- Centralized scheduling
- Clear dependency management
- Built-in retries & alerting
- Simplified operations
- Historical trend awareness
- Deterministic insight generation
- No time-drift between systems

### Data Engineering Layer (Apache Airflow)
#### The ETL layer is orchestrated using Apache Airflow, deployed via Docker.
### Responsibility
- Sales data is fetched via API calls from the upstream retail system.
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
- Snapshot-backed intelligence memory
- Numeric-grounded LLM outputs
- LLM used strictly as a structured rendering layer

### Reporting & Distribution Layer:

### Responsibilities
- PDF report generation
- Excel report creation
- WhatsApp automation
- Email distribution
- Duplicate prevention logic
- File-system bound execution
- Embedded KPI-based action recommendations


## Execution Flow

```mermaid
flowchart TD
    A[Airflow Scheduler] --> B[ETL Pipeline]
    B --> C[Product Update]
    C --> D[Daily Analysis]
    D --> E[Intelligence Engine]
    E --> F[LLM Recommendation Layer]
    F --> G[Daily WhatsApp]
    G --> H{Weekly?}
    H -->|Yes| I[Weekly Snapshot + Reports]
    I --> J[Weekly Mail]
    H -->|No| K[Skip Weekly]
    G --> L{Monthly?}
    L -->|Yes| M[Monthly Snapshot + Reports]
    M --> N[Monthly Mail]
    L -->|No| O[Skip Monthly]
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
- WoW revenue & quantity comparison
- Margin shift detection
- Risk scoring and anomaly detection
- Operational action recommendations

## Monthly Report
- Consolidated monthly business performance
- Strategic performance summary
- Consolidated monthly business performance
- MoM comparison
- Strategic performance summary
- Trend-aware intelligence insights


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
### Workflow
<img width="1538" height="616" alt="dags" src="https://github.com/user-attachments/assets/1f1d2b17-9f75-4413-9dfb-3ce48698807b" />

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

#### Observability & Monitoring

| Component     | Role                        |
| ------------- | --------------------------- |
| Prometheus    | Time-series metrics storage |
| Pushgateway   | Batch job metrics ingestion |
| Node Exporter | Infrastructure metrics      |
| Grafana       | Visualization dashboards    |


## CI/Automation
### GitHub Actions are configured for:
- Code validation
- Basic test execution
- Runtime automation is handled exclusively by Azure VM

## Design Principles
- Separation of concerns
- Deterministic data workflows
- Hybrid orchestration where appropriate
- Infrastructure-aware engineering decisions
- Production stability over tool overuse
- Minimal human operational dependency

## Final Note
### This project is not a traditional analytics dashboard.
### It is a production-grade automated sales intelligence engine designed for:

- Reliable analytics computation
- Deterministic insight generation
- Automated report delivery
- Minimal operational overhead

