package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import pojos.Score;

/**
 * 求平均成绩的自定义窗口聚合函数（增量聚合）
 */
public class Ex6_AvgScoreAggrFunc implements AggregateFunction<Score, Tuple2<Integer, Double>, String> {
    /**
     * 初始化中间累加器
     *
     * @return
     */
    @Override
    public Tuple2<Integer, Double> createAccumulator() {
        return Tuple2.of(0, 0.0);
    }

    /**
     * 来一条数据，就调用一次
     * 更新累加器
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public Tuple2<Integer, Double> add(Score value, Tuple2<Integer, Double> accumulator) {
        accumulator.f0 = accumulator.f0 + 1;
        accumulator.f1 += value.getScore();
        return accumulator;
    }

    /**
     * 返回最终结果
     * 窗口关闭触发时调用一次
     * @param accumulator
     * @return
     */
    @Override
    public String getResult(Tuple2<Integer, Double> accumulator) {

        return "本窗口计算的平均分结果是： " + accumulator.f1;
    }

    /**
     * 用于session窗口的内部窗口合并机制
     * 把两个累加器进行合并
     * @param a
     * @param b
     * @return
     */
    @Override
    public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
        a.f0 += b.f0;
        a.f1 += b.f1;
        return a;
    }
}