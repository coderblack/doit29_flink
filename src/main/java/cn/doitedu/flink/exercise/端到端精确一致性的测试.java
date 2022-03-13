package cn.doitedu.flink.exercise;

/**
 *
 * 从kafka读取 a,1 a,2 a,3 a,4 ... a,200
 *            b,1 b,2 b,3 b,4 ... b,200
 * 然后,对数据按字母keyBy
 * 然后，对整数做映射： (x,n) -> (x,10*n)
 * 然后, 将数据写入mysql
 *
 * 把容错相关配置打满！！
 * 在上述的一些算子中，安排一些 随机异常抛出（整个200条数据中大概抛出2次异常）
 *
 * 然后，观察你的mysql中的结果，是否正确（是否有重复或者有丢失）
 *
 */
public class 端到端精确一致性的测试 {


}
