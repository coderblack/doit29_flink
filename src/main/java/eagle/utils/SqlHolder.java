package eagle.utils;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/4
 **/
public class SqlHolder {

    public static final String DORIS_DETAIL_SINK_DDL
            = "CREATE TABLE doris_appdetail_sink (\n" +
            "  eventid                String                ,\n" +
            "  guid                   BIGINT                ,\n" +
            "  releasechannel         String                ,\n" +
            "  account                String                ,\n" +
            "  appid                  String                ,\n" +
            "  appversion             String                ,\n" +
            "  carrier                String                ,\n" +
            "  deviceid               String                ,\n" +
            "  devicetype             String                ,\n" +
            "  ip                     String                ,\n" +
            "  latitude               double                ,\n" +
            "  longitude              double                ,\n" +
            "  nettype                String                ,\n" +
            "  osname                 String                ,\n" +
            "  osversion              String                ,\n" +
            "  resolution             String                ,\n" +
            "  sessionid              String                ,\n" +
            "  `timestamp`            BIGINT                ,\n" +
            "  registerTime           BIGINT                ,\n" +
            "  firstAccessTime        BIGINT                ,\n" +
            "  isNew                  int                   ,\n" +
            "  geoHashCode            String                ,\n" +
            "  province               String                ,\n" +
            "  city                   String                ,\n" +
            "  region                 String                ,\n" +
            "  propsJson              String                ,\n" +
            "  dw_date                STRING                 \n" +
            ") \n" +
            "    WITH (\n" +
            "      'connector' = 'doris',\n" +
            "      'fenodes' = 'doit01:8030',\n" +
            "      'table.identifier' = 'dwd.app_log_detail',\n" +
            "      'username' = 'root',\n" +
            "      'password' = '123456'\n" +
            ")";


    public static final String  DORIS_DETAIL_SINK_DML
            = "INSERT INTO doris_appdetail_sink      \n" +
            "SELECT              \n" +
            "  eventid          ,\n" +
            "  guid             ,\n" +
            "  releasechannel   ,\n" +
            "  account          ,\n" +
            "  appid            ,\n" +
            "  appversion       ,\n" +
            "  carrier          ,\n" +
            "  deviceid         ,\n" +
            "  devicetype       ,\n" +
            "  ip               ,\n" +
            "  latitude         ,\n" +
            "  longitude        ,\n" +
            "  nettype          ,\n" +
            "  osname           ,\n" +
            "  osversion        ,\n" +
            "  resolution       ,\n" +
            "  sessionid        ,\n" +
            "  `timestamp`      ,\n" +
            "  registerTime     ,\n" +
            "  firstAccessTime  ,\n" +
            "  isNew            ,\n" +
            "  geoHashCode      ,\n" +
            "  province         ,\n" +
            "  city             ,\n" +
            "  region           ,\n" +
            "  propsJson        ,\n" +
            "  dw_date           \n" +
            "FROM logdetail"   ;

    public static final String KAFKA_DETAIL_SINK_DDL
            ="CREATE TABLE kafka_dwd_sink (\n" +
            "  eventid                String                ,\n" +
            "  guid                   BIGINT                ,\n" +
            "  releasechannel         String                ,\n" +
            "  account                String                ,\n" +
            "  appid                  String                ,\n" +
            "  appversion             String                ,\n" +
            "  carrier                String                ,\n" +
            "  deviceid               String                ,\n" +
            "  devicetype             String                ,\n" +
            "  ip                     String                ,\n" +
            "  latitude               double                ,\n" +
            "  longitude              double                ,\n" +
            "  nettype                String                ,\n" +
            "  osname                 String                ,\n" +
            "  osversion              String                ,\n" +
            "  resolution             String                ,\n" +
            "  sessionid              String                ,\n" +
            "  `timestamp`            BIGINT                ,\n" +
            "  registerTime           BIGINT                ,\n" +
            "  firstAccessTime        BIGINT                ,\n" +
            "  isNew                  int                   ,\n" +
            "  geoHashCode            String                ,\n" +
            "  province               String                ,\n" +
            "  city                   String                ,\n" +
            "  region                 String                ,\n" +
            "  propsJson              String                ,\n" +
            "  dw_date                STRING                 \n" +
            ") WITH (                                      \n" +
            "  'connector' = 'kafka',                      \n" +
            "  'topic' = 'dwd-applog-detail',              \n" +
            "  'properties.bootstrap.servers' = 'doit01:9092,doit02:9092,doit03:9092',    \n" +
            "  'properties.group.id' = 'dwdsink',          \n" +
            "  'scan.startup.mode' = 'latest-offset',      \n" +
            "  'format' = 'csv'                            \n" +
            ")";


    public static final String KAFKA_DETAIL_SINK_DML
            = "INSERT INTO kafka_dwd_sink      \n" +
                    "SELECT              \n" +
                    "  eventid          ,\n" +
                    "  guid             ,\n" +
                    "  releasechannel   ,\n" +
                    "  account          ,\n" +
                    "  appid            ,\n" +
                    "  appversion       ,\n" +
                    "  carrier          ,\n" +
                    "  deviceid         ,\n" +
                    "  devicetype       ,\n" +
                    "  ip               ,\n" +
                    "  latitude         ,\n" +
                    "  longitude        ,\n" +
                    "  nettype          ,\n" +
                    "  osname           ,\n" +
                    "  osversion        ,\n" +
                    "  resolution       ,\n" +
                    "  sessionid        ,\n" +
                    "  `timestamp`      ,\n" +
                    "  registerTime     ,\n" +
                    "  firstAccessTime  ,\n" +
                    "  isNew            ,\n" +
                    "  geoHashCode      ,\n" +
                    "  province         ,\n" +
                    "  city             ,\n" +
                    "  region           ,\n" +
                    "  propsJson        ,\n" +
                    "  dw_date           \n" +
                    "FROM logdetail"   ;
}
