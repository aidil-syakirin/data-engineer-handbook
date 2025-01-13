## XYZ Tech Data Engineering (DE) team : Aidil, Spencer, Thiru, Choo

# 1. Primary and secondary owners of the pipelines

Pipeline for experiments will be handled exclusively by DE team due to its projects status and will owned by a pair of DE team members. Profit and engagement pipeline will be handled primaly by Business Analytics (BA) team meanwhile growth pipeline will be by business analyst of Accounts team.  

Pipeline Name | Primary Owner | Secondary Owner
--- | --- | --- 
Unit-level profit for experiments | Aidil | Spencer
--- | --- | --- 
Daily growth for experiments | Thiru | Choo
--- | --- | --- 
Aggregate profit for investors | Business Analytic (BA) team | Data Engineering (DE) team
--- | --- | --- 
Aggregate engagement for investors | Business Analytic (BA) team | Data Engineering (DE) team
--- | --- | --- 
Aggregate growth for investors | Accounts team | Data Engineering (DE) team


2. On-call schedule for DE team 

Pipeline Name | Week 1 | Week 2 | Week 3 | Week 4

Unit-level profit for experiments | No on-call as it is not production pipeline and any issue will be rectified during office hours 

Daily growth for experiments | No on-call as it is not production pipeline and any issue will be rectified during office hours 

Aggregate profit for investors | Aidil | Thiru | Choo | Spencer

Aggregate engagement for investors | Aidil | Thiru | Choo | Spencer

Aggregate growth for investors | Spencer | Choo | Aidil | Thiru

Malaysia is one of the country that has the most public holidays per year, so if any team members is on holiday or on emergency leave, the next week members will cover accordingly. The team members also are from different races so hopefully no holiday clash.

3. Runbook for the business critical pipelines

A. Aggregate Profit for Investors

Description: Aggregates company-wide profit metrics to generate quarterly reports for investors.

Critical Inputs: Revenue and expense data from the accounting system.

Critical Outputs: Dashboard for financial analysis.

Common Failures:
Accounting API downtime.
Calculation errors (e.g., incorrect aggregation).
Data delivery delays to dashboards.

Monitoring Alerts:
Failure to complete by 9:00 am (next working day).
Discrepancy between expected and actual metrics.

Escalation:
Contact Primary: BA team
Further Escalation: DE team member on duty 

B. Aggregate Engagement for Investors

Description: This pipeline processes user engagement metrics (e.g., clicks, time spent, and actions taken) to create aggregate reports shared with investors.

Critical Inputs:

User activity logs from the tracking system.
Session metadata (e.g., session duration, device type).
External data on market trends (optional).

Critical Outputs:
Engagement metrics dashboard.
CSV/Excel files for investor reports.

Common Failures:
Missing Logs: If tracking systems fail to log data correctly, engagement metrics might be incomplete.
High Latency: Large datasets could cause the pipeline to exceed processing time limits.
Transformation Errors: Incorrect logic in calculating aggregated metrics, like averages or totals.
Schema Changes: Input data schema changes could break the pipeline.

Monitoring Alerts:
Data lagging by more than 16 hours.
Sudden drop/increase in engagement metrics compared to historical trends.
Pipeline failure notifications.

Escalation:
Contact Primary: BA team
Further Escalation: DE team member on duty 

C. Aggregate Growth for Investors

Description: This pipeline computes company-wide growth metrics (e.g., revenue growth, user acquisition rates) for inclusion in investor presentations.

Critical Inputs:
Revenue data from the accounting system.
User acquisition data from the CRM.
Historical growth benchmarks for context.

Critical Outputs:
Quarterly growth dashboards.
Reports delivered in PDF/Excel format for the investor relations team.

Common Failures:
Missing Data: Revenue or user acquisition data may not be available on time.
Incorrect Growth Rates: Errors in calculating growth percentages or comparing against baselines.
Data Duplication: Repeated data entries leading to inflated growth metrics.
API Downtime: Failure to pull data from accounting or CRM systems.

Monitoring Alerts:
Job runs longer than 2 hours.
Growth rate anomalies (e.g., negative or implausibly high growth).
Missing data for key metrics.

Escalation:
Contact Primary: Accounts team
Further Escalation: DE team member on duty 

D. The potential pipeline issues

Data Issues:
Missing or outdated data from source systems.
Incorrect formatting or schema changes.

Processing Issues:
ETL job failures due to resource limits (e.g., memory, CPU).
Logic errors in transformations (e.g., incorrect aggregations).

Infrastructure Issues:
Server or cloud resource outages.
Network connectivity problems.

Human Errors:
Accidental deletion or misconfiguration of pipeline jobs.

External Dependencies:
APIs or third-party services unavailable.


 
