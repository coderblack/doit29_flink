package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import cn.doitedu.flink.pojos.Score;

/**
 * 求分数之和，别的字段用最新的数据代替
 */
public class Ex6_ReduceFunc1 implements ReduceFunction<Score> {

    /**
     * value1 是上次的聚合结果
     * value2 是新进入窗口的一条数据
     * @param value1
     * @param value2
     * @return
     * @throws Exception
     */
    @Override
    public Score reduce(Score value1, Score value2) throws Exception {
        double scoreSum = value1.getScore() + value2.getScore();

        value2.setScore(scoreSum);

        return value2;
    }
}
