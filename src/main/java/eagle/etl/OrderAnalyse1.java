package eagle.etl;

import eagle.pojo.OmsOrderBean;
import lombok.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/7
 *  mysql业务库中的两表订单表的建表语句：
CREATE TABLE `oms_order` (
`id` int(11) NOT NULL,
`member_id` int(11) DEFAULT NULL,
`amount` double(255,0) DEFAULT NULL,
`pay_type` int(255) DEFAULT NULL,
`order_source` int(255) DEFAULT NULL,
`order_status` int(11) DEFAULT NULL,
`create_time` datetime DEFAULT NULL,
`update_time` datetime DEFAULT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 *
CREATE TABLE `oms_order_item` (
`id` int(11) NOT NULL,
`order_id` int(11) DEFAULT NULL,
`product_id` int(11) DEFAULT NULL,
`price` double DEFAULT NULL,
`quantity` int(11) DEFAULT NULL,
`coupon_amount` double DEFAULT NULL,
`product_category_id` int(11) DEFAULT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 *
 **/
public class OrderAnalyse1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        String omsOrderDDL =
                "CREATE TABLE flink_oms_order (                " +
                        "   id INT                             " +
                        "   ,member_id INT                     " +
                        "   ,amount double                     " +
                        "   ,pay_type int                      " +
                        "   ,order_source int                  " +
                        "   ,order_status int                  " +
                        "   ,create_time  timestamp(3)         " +
                        "   ,update_time  timestamp(3)         " +
                        "   ,ptime  as    proctime()           " +             // 表达式字段：  tableapi     schema.builder().columnByExpression("ptime","proctime()")
                        // "   ,watermark for update_time as update_time - interval '5' seconds" +   // 表达式字段：  tableapi     schema.builder().watermarkfor("update_time","update_time -interval '5' seconds ")
                        "   ,PRIMARY KEY(id) NOT ENFORCED      " +
                        ") WITH (                              " +
                        "   'connector' = 'mysql-cdc',         " +
                        "   'hostname' = 'doit01',             " +
                        "   'port' = '3306',                   " +
                        "   'username' = 'root',               " +
                        "   'password' = 'ABC123.abc123',      " +
                        "   'database-name' = 'eagle',         " +
                        "   'table-name' = 'oms_order'         " +
                        ")";

        String omsOrderItemDDl =
                "CREATE TABLE flink_oms_order_item (              " +
                        "   id INT                                " +
                        "   ,order_id INT                         " +
                        "   ,product_id INT                       " +
                        "   ,price   DOUBLE                       " +
                        "   ,quantity  INT                        " +
                        "   ,coupon_amount DOUBLE                 " +
                        "   ,product_category_id INT              " +
                        "   ,ptime   as    proctime()             " +
                        "   ,PRIMARY KEY(id) NOT ENFORCED         " +
                        ") WITH (                                 " +
                        "   'connector' = 'mysql-cdc',            " +
                        "   'hostname' = 'doit01',                " +
                        "   'port' = '3306',                      " +
                        "   'username' = 'root',                  " +
                        "   'password' = 'ABC123.abc123',         " +
                        "   'database-name' = 'eagle',            " +
                        "   'table-name' = 'oms_order_item'       " +
                        ")                                        ";

        // 创建sql中的cdc连接器表: oms_order
        tenv.executeSql(omsOrderDDL);
        /*tenv.executeSql("desc flink_oms_order").print();*/

        // 创建sql中的cdc连接器表: oms_order_item
        tenv.executeSql(omsOrderItemDDl);

        /**
         * 今天截止到当前的            订单总数，订单总额
         * 最近1小时的                订单总数，订单总额（每10分钟更新一次）
         * 最近1小时的订单总数，订单总额（每10分钟更新一次）
         * 截止到当前的各种支付方式下的 订单总数，订单总额
         * 截止到当前的各品类商品的     订单总数，购买总额
         * 最近1小时购买数量最多的前5个品类、商品（每10分钟更新一次）
         * 最近1小时购买金额最大的前5个品类、商品（每10分钟更新一次）
         * 最近1小时使用优惠券抵扣金额最多的前10个商品（每10分钟更新一次）
         */

        // 截止到当前的  订单总数，订单总额
        // 测试flinksql中对  date类型和 string类型进行比较的时候，是否会自动隐式转换
        // tenv.executeSql("select  DATE_FORMAT(create_time,'yyyy-MM-dd') ,current_date , DATE_FORMAT(create_time,'yyyy-MM-dd') = current_date from flink_oms_order").print();
        /*tenv.executeSql("select  count(1) as order_cnt ,sum(amount) as order_amt from flink_oms_order where  DATE_FORMAT(create_time,'yyyy-MM-dd')=current_date").print();*/


        // 最近 1小时的 订单总数，订单总额（每10分钟更新一次）
        // 直接在cdc的源表上进行 TVF窗口统计，是不支持的； （ TVF窗口聚合，不支持 cdc流中的 update和 delete变化 ）
        /*tenv.executeSql(
                "SELECT                                                                               " +
                "   window_start,                                                                              " +
                "   window_end,                                                                                " +
                "   count(1)  as order_cnt,                                                                    " +
                "   sum(amount) as  order_amt                                                                  " +
                "FROM TABLE(                                                                                   " +
                "  HOP(table flink_oms_order ,descriptor(ptime), interval '5' second ,interval '10' second)    " +
                ")                                                                                             " +
                "GROUP BY window_start,window_end                                                              ").print();*/

        // 所以，需要对cdc的源表进行转换，留下我们需要的 “变化”类型(+I)  数据
        Table table = tenv.from("flink_oms_order");
        // 将表对象，转成changelog流
        DataStream<Row> changelogStream = tenv.toChangelogStream(table);

        // 对changelog流中的数据进行过滤，只留下 rowKind=+I的数据
        SingleOutputStreamOperator<Row> filteredRowStream = changelogStream.filter(row -> row.getKind().toByteValue() == 0);

        // 将过滤好的row类型流，转成OmsOrderBean类型流
        /*SingleOutputStreamOperator<OmsOrderBean> omsBeanStream = filtered.map(row -> {
            Integer id = row.<Integer>getFieldAs(0);
            Integer member_id = row.<Integer>getFieldAs(1);
            double amount = row.<Double>getFieldAs(2);
            Integer pay_type = row.<Integer>getFieldAs(3);
            Integer order_source = row.<Integer>getFieldAs(4);
            Integer order_status = row.<Integer>getFieldAs(5);
            LocalDateTime create_time = row.<LocalDateTime>getFieldAs(6);
            LocalDateTime update_time = row.<LocalDateTime>getFieldAs(7);

            return new OmsOrderBean(id, member_id, amount, pay_type, order_source, order_status, create_time, update_time);
        });
        // 将 bean类型的流，转成 sql表
        tenv.createTemporaryView("t_order",omsBeanStream, Schema.newBuilder()
                .columnByExpression("ptime","proctime()")
                .build());
        */

        // 对于Row类型的datestream，转成sql视图时，可以不用先转成自定义bean，也能继续保留原来的schema
        tenv.createTemporaryView("t_order", filteredRowStream, Schema.newBuilder()
                .columnByExpression("pt", "proctime()")  // 原来的表结构中，就有ptime字段（但是到了这已经丢失了时间语义属性），重新定一个名字为pt的处理时间语义字段
                .build());
        /*tenv.executeSql("desc t_order").print();*/


        //  每10分钟统计一次最近1小时的订单总数，订单总额
        tenv.executeSql(
                "SELECT                                                                                " +
                "   window_start,                                                                               " +
                "   window_end,                                                                                 " +
                "   count(1)  as order_cnt,                                                                     " +
                "   sum(amount) as  order_amt                                                                   " +
                "FROM TABLE(                                                                                    " +
                "  HOP(table t_order ,descriptor(pt), interval '10' minute ,interval '1' hour)                  " +
                ")                                                                                              " +
                "GROUP BY window_start,window_end ")/*.print()*/;


        // 每10分钟统计一次最近 1小时的，各种支付方式下的  订单总数和订单总额
        tenv.executeSql(
                "SELECT                                                                               " +
                        "   window_start,                                                                      " +
                        "   window_end,                                                                        " +
                        "   pay_type,                                                                          " +
                        "   count(1)  as order_cnt,                                                            " +
                        "   sum(amount) as  order_amt                                                          " +
                        "FROM TABLE(                                                                           " +
                        "  HOP(table t_order ,descriptor(pt), interval '10' second ,interval '50' second)      " +
                        ")                                                                                     " +
                        "GROUP BY window_start,window_end,pay_type                                             ")/*.print()*/
        ;


        // TODO 每10分钟统计一次最近1小时的，各品类，各支付方式下的   订单总数和订单总额
        /**
         * 首先，该需求中，需要根据支付方式和商品类的维度来统计，而oms_order中有支付方式，oms_order_item中有商品和品类
         * 所以，需要将两个表先进行join，得到如下的宽表：
         * -- 订单id,支付方式,订单总额,商品id,商品品类,购买件数,商品单价
         *    o1,pay_type1,100,p1,c1,1,50
         *    o1,pay_type1,100,p2,c2,2,20
         *    o1,pay_type1,100,p3,c1,3,30
         *    o2,pay_type2,200,p4,c2,1,50
         *    o2,pay_type2,200,p2,c1,3,30
         *    o2,pay_type2,200,p3,c1,2,20
         */
        // 先把oms_order_item表的changelog流，过滤出 +I的数据
        Table omsOrderItemTable = tenv.from("flink_oms_order_item");
        SingleOutputStreamOperator<Row> appendOmsOrderItemStream = tenv.toChangelogStream(omsOrderItemTable)
                .filter(row -> row.getKind().toByteValue() == 0);
        tenv.createTemporaryView("t_order_item",appendOmsOrderItemStream, Schema.newBuilder()
                .columnByExpression("pt","proctime()")
                .build());

        /*tenv.executeSql(
                "SELECT                                                              \n" +
                        "   od.id,                                                   \n" +
                        "   od.pay_type,                                             \n" +
                        "   it.product_id,                                           \n" +
                        "   it.product_category_id,                                  \n" +
                        "   it.quantity,                                             \n" +
                        "   it.price                                                 \n" +
                        "FROM t_order od join t_order_item it on od.id=it.order_id   \n").print();*/

        Table joinedTable = tenv.sqlQuery(
                        "SELECT                                                                            \n" +
                        "   od.id,                                                                         \n" +
                        "   od.pay_type,                                                                   \n" +
                        "   it.product_id,                                                                 \n" +
                        "   it.product_category_id,                                                        \n" +
                        "   it.quantity,                                                                   \n" +
                        "   it.price                                                                       \n" +
                        "FROM t_order od join t_order_item it on od.id=it.order_id                         \n");
        tenv.createTemporaryView("order_detail",tenv.toDataStream(joinedTable), Schema.newBuilder()
                .columnByExpression("pt","proctime()")
                .build());

        /*tenv.executeSql("desc order_detail").print();*/

        tenv.executeSql(
                "SELECT                                                                                  \n" +
                        "    window_start,window_end,pay_type,product_category_id,                                \n" +
                        "    count(distinct id) as order_cnt,                                                     \n" +
                        "    sum(quantity * price ) as order_amt                                                  \n" +
                        "FROM TABLE(                                                                              \n" +
                        "  HOP(TABLE order_detail, descriptor(pt), interval '5' second ,interval '50' second)     \n" +
                        ")                                                                                        \n" +
                        "GROUP BY window_start,window_end,pay_type,product_category_id                            "
        ).print();

        env.execute();

    }


}
