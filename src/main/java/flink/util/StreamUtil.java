package flink.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {
    public static DataStream<String> getDataStream(StreamExecutionEnvironment env, ParameterTool parameterTool) {
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
            return null;
        }
       return dataStream;
    }
}
