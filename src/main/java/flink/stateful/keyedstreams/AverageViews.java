package flink.stateful.keyedstreams;

import flink.util.StreamUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageViews {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> inputStream = StreamUtil.getDataStream(env, parameterTool);

        if (inputStream == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> averageStream = inputStream
                .map(new RowSplitterMap())
                .keyBy(0)
                .reduce(new SumAndCount())
                .map(new Average());

        averageStream.print();
        env.execute("Average views.");
    }

    public static class RowSplitterMap implements MapFunction<String, Tuple3<String, Double, Integer>> {
        public Tuple3<String, Double, Integer> map(String input) {
            String fields[] = input.split(",");
            if (fields.length == 2) {
                return new Tuple3<>(
                  fields[0],
                  Double.parseDouble(fields[1]),
                  1
                );
            }
            return null;
        }
    }

    public static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {
        public Tuple3<String, Double, Integer> reduce (Tuple3<String, Double, Integer> cumulative,
                                                       Tuple3<String, Double, Integer> input) {
            return new Tuple3<>(
              input.f0,
              cumulative.f1 + input.f1,
              cumulative.f2 + input.f2
            );
        }
    }

    public static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) {
           return new Tuple2<>(
             input.f0, input.f1 / input.f2
           );
        }
    }
}