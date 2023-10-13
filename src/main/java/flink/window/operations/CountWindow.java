package flink.window.operations;

import flink.util.StreamUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Count window set the window size based on the number of entities exist with in that window.
 * Count windows in flink are applied to keyed streams, the count applies on per key basis.
 *
 * Note:- Here, the window applies on each key independently.
 */
public class CountWindow {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> inputStream = StreamUtil.getDataStream(env, parameterTool);

        if (inputStream == null) {
            System.exit(1);
            return;
        }

        DataStream<WordCount> wordCountDataStream = inputStream.flatMap(new WordCountSplitter())
                    .keyBy("word")
                    .countWindow(3)
                    .sum("count");

        wordCountDataStream.print();
        env.execute("Word count : Count window");
    }

    public static class WordCountSplitter implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String sentence, Collector<WordCount> collector) {
            for (String word : sentence.split(" ")) {
                collector.collect(new WordCount(word, 1));
            }
        }
    }

    public static class WordCount {

        public String word;
        public Integer count;

        public WordCount() {

        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}