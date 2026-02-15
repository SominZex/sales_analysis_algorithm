# Operations Runbook

This document describes **how to operate, monitor, and troubleshoot** the Sales Analysis Automation System in production.

---

## Normal Operation

### Daily
- ETL pipeline runs early morning
- Daily sales report generated
- PDF shared via Email and WhatsApp
![daily](https://github.com/user-attachments/assets/565ab63f-c6b8-4570-a68b-9e7308222119)

### Weekly (Monday)
- Weekly aggregation executed
- Weekly report emailed to stakeholders

### Monthly (1st of month)
- Monthly performance report generated
- Report emailed automatically

No manual action is required during normal operation.

---

## Key Directories

| Path | Purpose |
|----|--------|
| `airflow/dags/` | Orchestration and Data transformation |
| `logs/` | Execution and error logs |
| `monthly_query/` | Date logic for monthly reports |
| `.github/` | CI configuration |
| `docs/` | Documentation |

---

## Log Monitoring

Logs are the **primary monitoring mechanism**.

### What to Check
- Airflow UI for Orchestration log
- ETL logs for data ingestion failures
- Report generation logs for PDF issues
- Notification logs for delivery failures

Example:
```bash
tail -n 100 /home/azureuser/logs/etl_pip.log
```
