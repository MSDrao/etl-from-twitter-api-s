package flink.stateful.transformations;

import flink.util.StreamUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

public class CarSpeeds {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(300000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new FsStateBackend("file///tmp/checkpoints"));

        DataStream<String> inputStream = StreamUtil.getDataStream(env, parameterTool);

        if (inputStream == null) {
            System.exit(1);
            return;
        }

        DataStream<String> averageViewStream = inputStream
                .map(new speed())
                .keyBy(0)
                .flatMap(new AverageSpeedValueState());

        averageViewStream.print();
        env.execute("Average car speed.");
    }

    public static class speed implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String row) {
            return Tuple2.of(1, Double.parseDouble(row));
        }
    }

    public static class AverageSpeedValueState extends RichFlatMapFunction<Tuple2<Integer, Double>, String>{

        private transient ValueState<Tuple2<Integer, Double>> countSumState;

        @Override
        public void open(Configuration configuration) {
            ValueStateDescriptor<Tuple2<Integer, Double>> valueStateDescriptor =
                    new ValueStateDescriptor<>(
                      "carsAverageSpeed",
                      TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {}),
                      Tuple2.of(0, 0.0)
                    );
            countSumState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Double> input, Collector<String> collector)
                throws  Exception {
            Tuple2<Integer, Double> currentCountSum = countSumState.value();
            if (input.f1 >= 65) {
                collector.collect(String.format("EXCEEDED! The average speed of the last %s car(s) was, %s + " +
                        "yours speed is %s",
                        currentCountSum.f0,
                        currentCountSum.f1/currentCountSum.f0,
                        input.f1));
            countSumState.clear();
            currentCountSum = countSumState.value();
            } else {
                collector.collect("Thank you for staying under speed limit.");
            }
            currentCountSum.f0 += 1;
            currentCountSum.f1 += input.f1;
            countSumState.update(currentCountSum);
        }
    }
}