package flink.window.operations;

import flink.util.StreamUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * There is no fixed number of entities in a session window. Size of the session window is determined by the interval
 * between your sessions.
 *
 * For Ex:- Session can be determined as, if you pause for a minute then that's a new session window.
 * Here, we do not know how long the window size can be in terms of time or size.
 */
public class SessionWindow {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> inputStream = StreamUtil.getDataStream(env, parameterTool);

        if (inputStream == null) {
            System.exit(1);
            return;
        }

        // Here, a gap 10 seconds between subsequent messages implies a session.
        // If the gap is greater than 10 seconds, then the message which comes after
        // will be part of next session.
        DataStream<Tuple3<String, String, Double>> averageCountStream = inputStream
                .map(new RowSplitter())
                .keyBy(0, 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2);

        averageCountStream.print();
        env.execute();
    }

    public static class RowSplitter implements MapFunction<String, Tuple3<String, String, Double>> {
        public Tuple3<String, String, Double> map(String input) {
            String fields[] = input.split(",");
            if (fields.length == 2) {
                return new Tuple3<>(
                        fields[0],
                        fields[1],
                        Double.parseDouble(fields[2])
                );
            }
            return null;
        }
    }
}