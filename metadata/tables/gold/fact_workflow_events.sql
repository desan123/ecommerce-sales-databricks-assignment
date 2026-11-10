DROP TABLE IF EXISTS {{configs.config.gold_db}}.fact_workflow_events;
CREATE EXTERNAL TABLE {{configs.config.gold_db}}.fact_workflow_events (
    event_id        STRING    NOT NULL,
    application_id  STRING    NOT NULL,
    old_status      STRING,
    new_status      STRING,
    event_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (application_id)
LOCATION '{{ configs.config.gold_path }}/fact_workflow_events'
TBLPROPERTIES (
    'lakehouse.primary.key' = 'event_id',
    'lakehouse.foreign.keys' = 'application_id:{{configs.config.gold_db}}.fact_applications(application_id)',
    'delta.enableChangeDataFeed' = 'true'
)