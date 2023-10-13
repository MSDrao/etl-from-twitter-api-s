package flink.map.operations; /**
 * Anil Devarasetty : Nov 2020
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterStrings {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.
                getExecutionEnvironment();

        DataStream<String> dataStream = env
                .socketTextStream("localhost", 9999)
                .filter(new Filter());

        dataStream.print();

        env.execute("FilterFunction strings");
    }
}