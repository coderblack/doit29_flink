package cn.doitedu.flink.exercise;

/**
 * 先写一个数据模拟器，不断生成如下数据,并写入kafka
 * {"guid":1,"eventId":"addCart","channel":"app","timeStamp":1646784001000,"stayLong":200}
 * {"guid":1,"eventId":"share","channel":"h5","timeStamp":1646784002000,"stayLong":300}
 * {"guid":1,"eventId":"pageView","channel":"wxapp","timeStamp":1646784002000,"stayLong":260}
 *
 * 然后，开发flink程序，从kafka中消费上述数据，做如下处理：
 * 1.过滤掉所有来自于H5渠道的数据
 *
 * 2.每5秒钟统计一次最近5秒钟的每个渠道上的行为人数
 *
 * 3.每5秒钟算一次最近30秒的如下规则：
 *     一个人在最近30秒内addcart事件超过5次，则输出一个告警信息
 *     一个人在最近30秒内，如果出现某次事件的行为时长>300，则输出一个告警信息
 *     一个人如果发生了事件addCart事件后的5秒内没有做 payment事件，则输出一个“催支付”的信息
 *
 */
public class 阶段综合练习2 {

}
