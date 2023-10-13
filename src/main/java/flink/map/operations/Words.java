package flink.map.operations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Words {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream;

        if (parameterTool.has("input")) {
            System.out.println("Executing words with a simple file input.");
            dataStream = env.readTextFile(parameterTool.get("input"));
        } else if (parameterTool.has("host") && parameterTool.has("port")) {
            dataStream = env.socketTextStream(
                    parameterTool.get("host"), Integer.parseInt(parameterTool.get("port")));
        } else {
            System.out.println("Invalid input modes.");
            System.exit(1);
            return;
        }

        DataStream<String> wordStream = dataStream.flatMap(new Splitter());
        wordStream.print();

        env.execute("Word split.");

    }

    public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String sentence, Collector<String> output) throws Exception {
            String[] wordsList = sentence.split(" ");
            for (String word : wordsList) {
                output.collect(word);
            }
        }
    }
}