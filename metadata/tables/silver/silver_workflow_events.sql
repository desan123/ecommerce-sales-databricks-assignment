DROP TABLE IF EXISTS {{configs.config.silver_db}}.workflow_events;
CREATE EXTERNAL TABLE {{configs.config.silver_db}}.workflow_events (
    event_id        STRING    NOT NULL,
    application_id  STRING    NOT NULL,
    old_status      STRING,
    new_status      STRING,
    event_timestamp TIMESTAMP,
    anomaly_flag    BOOLEAN   NOT NULL,
    anomaly_reason  STRING
)
USING DELTA
LOCATION '{{ configs.config.silver_path }}/workflow_events'