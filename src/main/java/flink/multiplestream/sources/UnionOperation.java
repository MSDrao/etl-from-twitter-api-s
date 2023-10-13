package flink.multiplestream.sources;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionOperation {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> stream_1 = env.socketTextStream("localhost", 8000);
        DataStream<String> stream_2 = env.socketTextStream("localhost", 9000);

        if (stream_1 == null || stream_2 == null) {
            System.exit(1);
            return ;
        }

        DataStream<String> unionStream = stream_1.union(stream_2);
        unionStream.print();

        env.execute("Union operation");
    }
}
