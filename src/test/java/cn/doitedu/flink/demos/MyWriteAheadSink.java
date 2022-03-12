package cn.doitedu.flink.demos;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

public class MyWriteAheadSink extends GenericWriteAheadSink<String> {


    public MyWriteAheadSink(CheckpointCommitter committer, TypeSerializer<String> serializer, String jobID) throws Exception {
        super(committer, serializer, jobID);
    }

    @Override
    protected boolean sendValues(Iterable<String> values, long checkpointId, long timestamp) throws Exception {
        return false;
    }
}
