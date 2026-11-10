DROP TABLE IF EXISTS {{configs.config.bronze_db}}.workflow_events;
CREATE EXTERNAL TABLE {{configs.config.bronze_db}}.workflow_events (
    application_id  STRING,
    old_status      STRING,
    new_status      STRING,
    event_timestamp STRING
)
USING DELTA
LOCATION '{{ configs.config.bronze_path }}/events'
COMMENT 'Bronze workflow events table'
