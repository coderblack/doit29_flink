-- 1,1345736548231,addcart,10
--sql_1
CREATE TABLE action_event
(
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
--sql_2
-- 1,1345736548231,addcart,10,app
-- 1,1345736548231,pageview,20,wxapp
CREATE TABLE action_event
(
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
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      )

    ~
--sql_3  筛选出行为时长>50的数据
SELECT guid
     , action_time
     , event_id
     , action_timelong * 1000 as action_timelong
FROM action_event
WHERE action_timelong > 50 ~
--sql_4 访客总数
select count(distinct guid) as uv
from action_event ~
--sql_5 各渠道的访客总数
select channel, count(distinct guid) as uv
from action_event
group by channel ~
--sql_6 各事件的发生人数
select event_id, count(distinct guid) as uv
from action_event
group by event_id ~
--sql_7 各事件的行为总时长
select event_id, sum(action_timelong) as sum_actiontime
from action_event
group by event_id ~
--sql_8 各渠道的行为事件种类数
select channel, count(distinct event_id) as events
from action_event
group by channel ~
--sql_9 各渠道中，行为总时长最大的前5个人及其行为总时长
select channel
     , guid
     , sum_actiontime
from (
         -- 在按不同的渠道，对渠道内用户根据时长排序打行号
         select channel
              , guid
              , sum_actiontime
              , row_number() over(partition by channel order by sum_actiontime desc ) as rn
         from (
                  -- 先求每个渠道中，每个人的行为总时长
                  select channel
                       , guid
                       , sum(action_timelong) as sum_actiontime
                  from action_event
                  group by channel, guid
              ) AS o1
     ) AS o2
where rn <= 5 ~
--sql_10 定义一个连接kafka数据的带事件时间语义属性和watermark的表
CREATE TABLE action_event
(
    `guid`            BIGINT,
    `action_time`     BIGINT,
    `event_id`        STRING,
    `action_timelong` BIGINT,
    `channel`         STRING,
    `proctime` as proctime(),                                              -- 用一个表达式逻辑字段来声明processing-time属性，其中的表达式函数 proctime()，仅仅是一个标记函数
    `action_time_ltz` as to_timestamp_ltz(action_time,3),                  -- 用 as 表达式的语法，定义了一个逻辑字段
    watermark for action_time_ltz as action_time_ltz - interval '0' second -- 用转成了timestamp类型的逻辑字段，指定为事件时间，以及指定watermark生成策略（不乱序）
) WITH (
      'connector' = 'kafka',
      'topic' = 'flinksql-01',
      'properties.bootstrap.servers' = 'doit01:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      )


    ~
--sql_11 通过数据源提供的元数据来声明表字段
CREATE TABLE action_event
(
    `guid`            BIGINT,
    `action_time`     BIGINT,
    `event_id`        STRING,
    `action_timelong` BIGINT,
    `channel`         STRING,
    `tpc`             STRING METADATA from 'topic',
    `partition`       INT METADATA, -- 声明的字段类型和底层的元数据的key同名，则不用专门指定 from  'key'
    `offset`          BIGINT metadata from 'offset'
) WITH (
      'connector' = 'kafka',
      'topic' = 'flinksql-01',
      'properties.bootstrap.servers' = 'doit01:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      )

    ~
-- sql_12 高阶聚合函数举例
select channel,
       event_id,
       count(distinct guid) as uv
from action_event
group by cube (channel, event_id)
    ~
-- sql_13 窗口聚合案例
-- 每 2秒，计算一次最近6秒的 各渠道的 uv 数
select window_start
     , window_end
     , window_time
     , channel
     , count(distinct guid) as uv
from table(
        hop(table action_event ,descriptor(action_time_ltz), interval '2' seconds , interval '6' seconds)
    )
group by window_start, window_end, window_time, channel ~
-- sql_14 窗口聚合案例
-- 每2秒，计算一次最近2秒的，各渠道的，uv数
-- 可以用滑动窗口，也可以用滚动窗口
select window_start
     , window_end
     , channel
     , count(distinct guid) as uv
from table(
        tumble(table action_event ,descriptor(action_time_ltz), interval '2' seconds)
    )
group by window_start, window_end, channel ~
-- sql_15 窗口聚合案例
-- 每2秒，计算一次今天0.00以来的，各渠道的，累计uv数
-- cumulate窗口
select window_start
     , window_end
     , channel
     , count(distinct guid) as uv
from table(
        cumulate(table action_event ,descriptor(action_time_ltz), interval '2' seconds, interval '24' hours)
    )
group by window_start, window_end, channel

~
-- sql_16  窗口join示例
select o1.id
     , o1.name
     , o2.phone
     , o1.window_start
     , o1.window_end
from (
         select id,
                name,
                window_start,
                window_end
         from table(
                 tumble(table t1,descriptor(rt),interval '5' second))
     ) o1

         join

     (
         select id,
                phone,
                window_start,
                window_end
         from table(
                 tumble(table t2,descriptor(rt),interval '5' second))
     ) o2
     on o1.id = o2.id and o1.window_start = o2.window_start and o1.window_end = o2.window_end


