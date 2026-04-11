# Databricks notebook source
# DBTITLE 1,Monitoring Overview
# MAGIC %md
# MAGIC # NovaFinds Pipeline Monitoring Dashboard
# MAGIC
# MAGIC **Purpose**: Monitor the NovaFinds Lakeflow Pipeline health, data quality, and performance.
# MAGIC
# MAGIC **Refresh Schedule**: Run this notebook hourly (after pipeline completes) or on-demand for investigations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Monitoring Sections
# MAGIC
# MAGIC 1. **Pipeline Health** - Run status, failures, duration trends
# MAGIC 2. **Data Quality Expectations** - Pass/fail rates for hard/soft blocks
# MAGIC 3. **Row Count Deltas** - Detect dropped rows (soft block violations)
# MAGIC 4. **Connector Health** - Lakeflow Connect & Fivetran sync status
# MAGIC 5. **Alerts & Anomalies** - Automatic detection of issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **⚙️ Configuration Required**:
# MAGIC - Update `pipeline_id` variable below with your pipeline ID
# MAGIC - Update `catalog` and `schema` to match your Unity Catalog location

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
pipeline_id = "<YOUR_PIPELINE_ID>"  # Find this in DLT UI URL
catalog = "main"
schema = "novafinds"
target_schema = f"{catalog}.{schema}"

print(f"✅ Monitoring pipeline: {pipeline_id}")
print(f"✅ Target schema: {target_schema}")

# COMMAND ----------

# DBTITLE 1,Pipeline Run History
# MAGIC %sql
# MAGIC -- 1. Pipeline Run History (Last 24 Hours)
# MAGIC -- Shows all pipeline updates with status, duration, and errors
# MAGIC
# MAGIC SELECT 
# MAGIC   update_id,
# MAGIC   timestamp,
# MAGIC   CASE 
# MAGIC     WHEN state = 'COMPLETED' THEN '✅ SUCCESS'
# MAGIC     WHEN state = 'FAILED' THEN '❌ FAILED'
# MAGIC     WHEN state = 'RUNNING' THEN '🔄 RUNNING'
# MAGIC     ELSE state
# MAGIC   END AS status,
# MAGIC   ROUND((end_time - start_time) / 60, 1) AS duration_minutes,
# MAGIC   CASE 
# MAGIC     WHEN cause = 'USER_ACTION' THEN 'Manual'
# MAGIC     WHEN cause = 'SCHEDULE' THEN 'Scheduled'
# MAGIC     ELSE cause
# MAGIC   END AS trigger_type,
# MAGIC   error_message
# MAGIC FROM event_log("${pipeline_id}")
# MAGIC WHERE event_type = 'update_progress'
# MAGIC   AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Data Quality Expectations
# MAGIC %sql
# MAGIC -- 2. Data Quality Expectations Summary
# MAGIC -- Shows pass/fail rates for all EXPECT constraints
# MAGIC
# MAGIC WITH latest_update AS (
# MAGIC   SELECT MAX(update_id) AS update_id
# MAGIC   FROM event_log("${pipeline_id}")
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
# MAGIC ),
# MAGIC expectation_stats AS (
# MAGIC   SELECT 
# MAGIC     dataset AS table_name,
# MAGIC     name AS expectation_name,
# MAGIC     SUM(passed_records) AS passed_count,
# MAGIC     SUM(failed_records) AS failed_count,
# MAGIC     ROUND(SUM(passed_records) / NULLIF(SUM(passed_records) + SUM(failed_records), 0) * 100, 2) AS pass_rate_pct
# MAGIC   FROM event_log("${pipeline_id}")
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND update_id IN (SELECT update_id FROM latest_update)
# MAGIC     AND expectations IS NOT NULL
# MAGIC   LATERAL VIEW explode(expectations) AS expectation
# MAGIC   GROUP BY dataset, name
# MAGIC )
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   expectation_name,
# MAGIC   passed_count,
# MAGIC   failed_count,
# MAGIC   pass_rate_pct,
# MAGIC   CASE 
# MAGIC     WHEN failed_count = 0 THEN '✅ PASS'
# MAGIC     WHEN pass_rate_pct >= 95 THEN '⚠️ WARNING'
# MAGIC     ELSE '❌ CRITICAL'
# MAGIC   END AS status
# MAGIC FROM expectation_stats
# MAGIC ORDER BY pass_rate_pct ASC, failed_count DESC

# COMMAND ----------

# DBTITLE 1,Row Count Deltas
# MAGIC %sql
# MAGIC -- 3. Row Count Deltas (Detect Dropped Rows)
# MAGIC -- Compare Bronze vs Silver layer to see impact of soft blocks
# MAGIC
# MAGIC WITH latest_metrics AS (
# MAGIC   SELECT 
# MAGIC     origin.dataset AS table_name,
# MAGIC     SUM(num_output_rows) AS row_count
# MAGIC   FROM event_log("${pipeline_id}")
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
# MAGIC   GROUP BY origin.dataset
# MAGIC )
# MAGIC SELECT 
# MAGIC   b.table_name AS bronze_table,
# MAGIC   b.row_count AS bronze_rows,
# MAGIC   s.row_count AS silver_rows,
# MAGIC   (b.row_count - s.row_count) AS rows_dropped,
# MAGIC   ROUND((b.row_count - s.row_count) / NULLIF(b.row_count, 0) * 100, 2) AS drop_rate_pct,
# MAGIC   CASE 
# MAGIC     WHEN (b.row_count - s.row_count) / NULLIF(b.row_count, 0) > 0.3 THEN '❌ HIGH DROP RATE'
# MAGIC     WHEN (b.row_count - s.row_count) / NULLIF(b.row_count, 0) > 0.1 THEN '⚠️ MODERATE'
# MAGIC     ELSE '✅ OK'
# MAGIC   END AS status
# MAGIC FROM latest_metrics b
# MAGIC LEFT JOIN latest_metrics s
# MAGIC   ON REPLACE(b.table_name, '_bronze', '_silver') = s.table_name
# MAGIC WHERE b.table_name LIKE '%_bronze'
# MAGIC ORDER BY drop_rate_pct DESC

# COMMAND ----------

# DBTITLE 1,Latest Run Summary
# MAGIC %sql
# MAGIC -- 4. Latest Pipeline Run Summary
# MAGIC -- High-level overview of the most recent update
# MAGIC
# MAGIC WITH latest_run AS (
# MAGIC   SELECT 
# MAGIC     update_id,
# MAGIC     timestamp AS start_time,
# MAGIC     state,
# MAGIC     end_time,
# MAGIC     ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(timestamp)) / 60, 1) AS duration_minutes
# MAGIC   FROM event_log("${pipeline_id}")
# MAGIC   WHERE event_type = 'update_progress'
# MAGIC   ORDER BY timestamp DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC tables_updated AS (
# MAGIC   SELECT COUNT(DISTINCT origin.dataset) AS table_count
# MAGIC   FROM event_log("${pipeline_id}"), latest_run
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND event_log.update_id = latest_run.update_id
# MAGIC ),
# MAGIC total_rows_processed AS (
# MAGIC   SELECT SUM(num_output_rows) AS total_rows
# MAGIC   FROM event_log("${pipeline_id}"), latest_run
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND event_log.update_id = latest_run.update_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   lr.update_id,
# MAGIC   lr.start_time,
# MAGIC   lr.state AS status,
# MAGIC   lr.duration_minutes,
# MAGIC   tu.table_count AS tables_updated,
# MAGIC   tr.total_rows AS total_rows_processed,
# MAGIC   CASE 
# MAGIC     WHEN lr.state = 'COMPLETED' AND lr.duration_minutes < 30 THEN '✅ HEALTHY'
# MAGIC     WHEN lr.state = 'COMPLETED' AND lr.duration_minutes >= 30 THEN '⚠️ SLOW'
# MAGIC     WHEN lr.state = 'FAILED' THEN '❌ FAILED'
# MAGIC     ELSE '🔄 IN PROGRESS'
# MAGIC   END AS health_status
# MAGIC FROM latest_run lr
# MAGIC CROSS JOIN tables_updated tu
# MAGIC CROSS JOIN total_rows_processed tr

# COMMAND ----------

# DBTITLE 1,Anomaly Detection
# MAGIC %sql
# MAGIC -- 5. Anomaly Detection: Compare Latest Run vs Historical Average
# MAGIC -- Detect unusual patterns (duration spikes, row count drops)
# MAGIC
# MAGIC WITH historical_stats AS (
# MAGIC   SELECT 
# MAGIC     AVG(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(timestamp)) / 60 AS avg_duration_minutes,
# MAGIC     STDDEV(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(timestamp)) / 60 AS stddev_duration_minutes
# MAGIC   FROM event_log("${pipeline_id}")
# MAGIC   WHERE event_type = 'update_progress'
# MAGIC     AND state = 'COMPLETED'
# MAGIC     AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC ),
# MAGIC latest_run AS (
# MAGIC   SELECT 
# MAGIC     (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(timestamp)) / 60 AS duration_minutes
# MAGIC   FROM event_log("${pipeline_id}")
# MAGIC   WHERE event_type = 'update_progress'
# MAGIC     AND state = 'COMPLETED'
# MAGIC   ORDER BY timestamp DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   ROUND(h.avg_duration_minutes, 1) AS avg_duration_7d,
# MAGIC   ROUND(l.duration_minutes, 1) AS latest_duration,
# MAGIC   ROUND((l.duration_minutes - h.avg_duration_minutes) / h.avg_duration_minutes * 100, 1) AS duration_change_pct,
# MAGIC   CASE 
# MAGIC     WHEN l.duration_minutes > h.avg_duration_minutes + (2 * h.stddev_duration_minutes) THEN '❌ ANOMALY: Duration spike'
# MAGIC     WHEN l.duration_minutes < h.avg_duration_minutes * 0.5 THEN '⚠️ WARNING: Unusually fast (data missing?)'
# MAGIC     ELSE '✅ NORMAL'
# MAGIC   END AS anomaly_status
# MAGIC FROM historical_stats h
# MAGIC CROSS JOIN latest_run l

# COMMAND ----------

# DBTITLE 1,Connector Health
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Connector Health Monitoring
# MAGIC
# MAGIC ### Lakeflow Connect (PostgreSQL CDC)
# MAGIC
# MAGIC To monitor Lakeflow Connect CDC lag and health:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check CDC lag (time between source change and Bronze table update)
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   MAX(_commit_timestamp) AS latest_cdc_commit,
# MAGIC   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS lag_minutes
# MAGIC FROM main.lakeflow_postgres.*  -- Replace with your schema
# MAGIC GROUP BY table_name
# MAGIC HAVING lag_minutes > 10  -- Alert if lag > 10 minutes
# MAGIC ```
# MAGIC
# MAGIC ### Fivetran (Stripe)
# MAGIC
# MAGIC To monitor Fivetran sync status:
# MAGIC 1. **Fivetran Dashboard**: Check connector status at https://fivetran.com/dashboard/connectors
# MAGIC 2. **Sync History**: Look for failed syncs or unusual delays
# MAGIC 3. **API Monitoring** (optional): Use Fivetran API to query sync status programmatically
# MAGIC
# MAGIC ```python
# MAGIC # Example: Fivetran API health check (requires API key)
# MAGIC import requests
# MAGIC
# MAGIC headers = {"Authorization": f"Bearer {fivetran_api_key}"}
# MAGIC response = requests.get(
# MAGIC     "https://api.fivetran.com/v1/connectors/{connector_id}",
# MAGIC     headers=headers
# MAGIC )
# MAGIC connector_status = response.json()["data"]["status"]["sync_state"]
# MAGIC print(f"Fivetran Sync State: {connector_status}")  # Should be 'syncing' or 'scheduled'
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Alert Configuration
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 Alert Configuration Checklist
# MAGIC
# MAGIC ### ✅ Configured in DLT UI:
# MAGIC - [ ] **Pipeline Failure Alert** → Email to: _________________
# MAGIC - [ ] **Expectation Failure Alert** → Email to: _________________
# MAGIC - [ ] **Duration Threshold Alert** → Trigger if > 30 minutes
# MAGIC
# MAGIC ### ✅ Schedule This Monitoring Notebook:
# MAGIC - [ ] **Frequency**: Every hour (10 minutes after pipeline runs)
# MAGIC - [ ] **Compute**: Serverless or small cluster
# MAGIC - [ ] **Email Results**: Check "Email on completion" if anomalies found
# MAGIC
# MAGIC ### ✅ Slack Integration (Optional):
# MAGIC 1. Create Slack incoming webhook: https://api.slack.com/messaging/webhooks
# MAGIC 2. Add webhook URL to DLT pipeline notifications
# MAGIC 3. Test with manual pipeline run
# MAGIC
# MAGIC ### ✅ PagerDuty Integration (Optional):
# MAGIC 1. Create PagerDuty integration: https://support.pagerduty.com/docs/services-and-integrations
# MAGIC 2. Use webhook URL in DLT notifications for critical alerts
# MAGIC 3. Configure escalation policy for failed pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Recommended Alert Thresholds
# MAGIC
# MAGIC | Metric | Warning | Critical |
# MAGIC |--------|---------|----------|
# MAGIC | Pipeline duration | >30 min | >60 min |
# MAGIC | Expectation pass rate | <95% | <90% |
# MAGIC | Row drop rate (customer_silver) | >30% | >50% |
# MAGIC | CDC lag (Lakeflow Connect) | >10 min | >30 min |
# MAGIC | Failed runs in 24h | 1 | 3+ |
# MAGIC
# MAGIC Adjust these based on your SLAs and data volume.