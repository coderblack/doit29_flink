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
-- 1,1345736548231,addcart,10,app
-- 1,1345736548231,pageview,20,wxapp
CREATE TABLE action_event (
      `guid`            BIGINT,
      `action_time`     BIGINT,
      `event_id`        STRING,
      `action_timelong` BIGINT,
      `channel`         STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'flinksql-01',
      'properties.bootstrap.servers' = 'doit01:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'csv'
      )

~
SELECT
   guid
   ,action_time
   ,event_id
   ,action_timelong *1000 as action_timelong
FROM action_event
WHERE action_timelong > 50
