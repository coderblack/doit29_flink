package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * 普通group by分组聚合
 *
 * 窗口TVF语法聚合
 */
public class _07_GroupAggregateQuery {

    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * CREATE TABLE action_event
         * (
         *     `guid`            BIGINT,
         *     `action_time`     BIGINT,
         *     `event_id`        STRING,
         *     `action_timelong` BIGINT,
         *     `channel`         STRING,
         *     `proctime` as proctime(),                                              -- 用一个表达式逻辑字段来声明processing-time属性，其中的表达式函数 proctime()，仅仅是一个标记函数
         *     `action_time_ltz` as to_timestamp_ltz(action_time,3),                  -- 用 as 表达式的语法，定义了一个逻辑字段
         *     watermark for action_time_ltz as action_time_ltz - interval '0' second -- 用转成了timestamp类型的逻辑字段，指定为事件时间，以及指定watermark生成策略（不乱序）
         * ) WITH (
         *       'connector' = 'kafka',
         *       'topic' = 'flinksql-01',
         *       'properties.bootstrap.servers' = 'doit01:9092',
         *       'properties.group.id' = 'testGroup',
         *       'scan.startup.mode' = 'latest-offset',
         *       'format' = 'csv',
         *       'csv.ignore-parse-errors' = 'true'
         *       )
         */
        // 执行 sql 建kafka表
        tenv.executeSql(SqlHolder.getSql(10));

        // 查询
        //tenv.executeSql("select * from action_event").print();
        /* *
         * +----+-----+---------------+-----------+----------------------+---------+-------------------------+-------------------------+
         * | op |guid |   action_time |  event_id |      action_timelong | channel |                proctime |         action_time_ltz |
         * +----+-----+---------------+-----------+----------------------+---------+-------------------------+-------------------------+
         * | +I |   2 | 1647598533000 |  pageview |                  400 |   wxapp | 2022-03-19 15:51:33.213 | 2022-03-18 18:15:33.000 |
         */

        /**
         * 多维分析查询：
         *    每个渠道中，每种事件的用户数（uv）
         *    每个渠道中的用户数（uv）
         *    每种事件的用户数（uv）
         *    总的用户数（uv）
         */
        // 没有高阶聚合函数的时候，就得如此麻烦：
        // tenv.executeSql("select channel,event_id,count(distinct guid) as uv  from action_event group by channel,event_id").print();
        // tenv.executeSql("select channel,count(distinct guid) as uv  from action_event group by channel").print();
        // tenv.executeSql("select event_id,count(distinct guid) as uv  from action_event group by event_id").print();
        // tenv.executeSql("select count(distinct guid) as uv  from action_event").print();

        // 有了高阶聚合函数，就如此easy：
        tenv.executeSql(SqlHolder.getSql(12))/*.print()*/;


        /**
         * TVF 窗口聚合语法测试
         *
         * 每 2秒，计算一次最近5秒的 各渠道的uv数
         */
        tenv.executeSql(SqlHolder.getSql(13))/*.print()*/;

        /**
         * 每2秒，计算一次最近2秒的，各渠道的，uv数
         */
        tenv.executeSql(SqlHolder.getSql(14))/*.print()*/;

        /**
         * 每2秒，计算一次今天0.00以来的，各渠道的，累计uv数
         * 测试数据：
         * 2022-03-19 00:00:01.000
         * 1,1647619201000,pageview,400,wxapp
         * 1,1647619201000,pageview,400,app
         * 2,1647619202000,pageview,400,wxapp
         * 2,1647619202000,pageview,400,app
         * 3,1647619203000,pageview,400,wxapp
         * 3,1647619203000,pageview,400,app
         * 4,1647619204000,pageview,400,wxapp
         * 4,1647619204000,pageview,400,app
         */
        tenv.executeSql(SqlHolder.getSql(15)).print();
        /**
         * +----+-------------------------+-------------------------+----------+------+
         * | op |            window_start |              window_end |  channel |   uv |
         * +----+-------------------------+-------------------------+----------+------+
         * | +I | 2022-03-19 00:00:00.000 | 2022-03-19 00:00:02.000 |    wxapp |    1 |
         * | +I | 2022-03-19 00:00:00.000 | 2022-03-19 00:00:02.000 |      app |    1 |
         * | +I | 2022-03-19 00:00:00.000 | 2022-03-19 00:00:04.000 |    wxapp |    3 |
         * | +I | 2022-03-19 00:00:00.000 | 2022-03-19 00:00:04.000 |      app |    3 |
         */



    }
}
