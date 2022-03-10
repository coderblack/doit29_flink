package cn.doitedu.flink.functions;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Ex6_DataSource implements SourceFunction<String> {

    /**
     *  int id;
     *  String gender;
     *  double score;
     *  long timeStamp;
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        while(true) {
            int id = RandomUtils.nextInt(1, 1000);
            String gender = RandomStringUtils.random(1, "fm");
            double score = RandomUtils.nextDouble(60.0, 100.0);
            long timeStamp = System.currentTimeMillis();

            ctx.collect(String.format("%d,%s,%.2f,%d",id,gender,score,timeStamp));
            Thread.sleep(RandomUtils.nextInt(500,3000));
        }
    }

    @Override
    public void cancel() {

    }
}
