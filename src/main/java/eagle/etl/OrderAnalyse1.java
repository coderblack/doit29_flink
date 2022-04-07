package eagle.etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

        tenv.executeSql(omsOrderItemDDl);
        tenv.executeSql("select * from flink_oms_order_item").print();

        /**
         * 截止到当前的                订单总数，订单总额
         * 截止到当前的各品类商品的     订单总数，购买总额
         * 截止到当前的各种支付方式下的 订单总数，订单总额
         * 最近1小时的订单总数，订单总额（每10分钟更新一次）
         * 最近1小时购买数量最多的前5个品类、商品（每10分钟更新一次）
         * 最近1小时购买金额最大的前5个品类、商品（每10分钟更新一次）
         * 最近1小时使用优惠券抵扣金额最多的前10个商品（每10分钟更新一次）
         */


    }
}
