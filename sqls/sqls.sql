-- 1,1345736548231,addcart,10
CREATE TABLE action_event (
   `guid`            BIGINT,
   `action_time`     BIGINT,
   `event_id`        STRING,
   `action_timelong` BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'flinksql-01',
      'properties.bootstrap.servers' = 'doit01:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'csv'
)

~
