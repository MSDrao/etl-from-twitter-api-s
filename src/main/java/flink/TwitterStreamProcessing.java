package flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;

public class TwitterStreamProcessing {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        if (!parameterTool.has(TwitterSource.CONSUMER_KEY) ||
            !parameterTool.has(TwitterSource.CONSUMER_SECRET) ||
            !parameterTool.has(TwitterSource.TOKEN) ||
            !parameterTool.has(TwitterSource.TOKEN_SECRET)) {

            System.out.println("Mandatory fields are missing.");

            System.exit(1);
            return;
        }

        DataStream<String> sourceStream = env.addSource(new TwitterSource(parameterTool.getProperties()));
        DataStream<Tuple2<String, Integer>> tweets = sourceStream
                .flatMap(new LanguageCount())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .sum(1);

        tweets.print();
        env.execute("Twitter Stream Language Count.");
    }

    public static class LanguageCount implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private transient ObjectMapper jsonParser;
        public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {

            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            JsonNode jsonNode = jsonParser.readValue(input, JsonNode.class);
            String language = jsonNode.has("user") && jsonNode.get("user").has("lang") ?
                    jsonNode.get("user").get("lang").getValueAsText() : "unknown";
            collector.collect(new Tuple2<String, Integer>(
                    language,
                    1
            ));
        }
    }
}