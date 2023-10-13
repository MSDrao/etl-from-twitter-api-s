package flink.window.operations;

import flink.util.StreamUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * Sliding window which slides over the data. Difference from the tumbling window is, it allows the entity can be present
 * in more than one window. Sliding window is exactly same like tumbling window in that window size if fixed and specified
 * in the form of a time interval. The important point to remember is, it allows the overlapping time. This time overlapping
 * between consecutive windows is called sliding interval.
 */
public class SlidingWindow {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> inputStream = StreamUtil.getDataStream(env, parameterTool);

        if (inputStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Integer>> wordCountStream = inputStream
                .flatMap(new WordCountSplitter()).keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .sum(1);

        wordCountStream.print();
        env.execute("Word count : Sliding window");
    }

    public static class WordCountSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
