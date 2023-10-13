package flink.multiplestream.sources;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class JoinStreams {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<Tuple2<String, String>> salariesStream = env
                .socketTextStream("localhost", 8000).map(new NameDetailsSplitter());
        DataStream<Tuple2<String, String>> deptStreams = env
                .socketTextStream("localhost", 9000).map(new NameDetailsSplitter());

        if (salariesStream == null || deptStreams == null) {
            System.exit(1);
            return ;
        }

        DataStream<Tuple3<String, String, String>> joinedStream = salariesStream
                .join(deptStreams)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new EmployeeDetailFunction());
        joinedStream.print();

        env.execute("Window Join Example.");
    }

    private static class NameDetailsSplitter implements MapFunction<String, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(String s) {
            String[] fields = s.split(" ");
            return new Tuple2<>(
                    fields[0],
                    fields[1]
            );
        }
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, String>, String> {
        @Override
        public String getKey(Tuple2<String, String> input) {
            return input.f0;
        }
    }

    private static class EmployeeDetailFunction implements JoinFunction<
            Tuple2<String, String>,
            Tuple2<String, String>,
            Tuple3<String, String, String>> {
        @Override
        public Tuple3<String, String, String> join(Tuple2<String, String> first, Tuple2<String, String> second) {
            return Tuple3.of(first.f0, first.f1, second.f1);
        }
    }
}