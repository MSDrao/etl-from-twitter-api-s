package flink.map.operations;

import org.apache.flink.api.common.functions.MapFunction;

public class RoundMap implements MapFunction<String, Long> {
    public Long map(String input) {
        return Math.round(Double.parseDouble(input));
    }
}