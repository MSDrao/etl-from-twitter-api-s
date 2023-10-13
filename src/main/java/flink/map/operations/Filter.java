package flink.map.operations; /**
 * Anil Devarasetty : Nov 2020
 */

import org.apache.flink.api.common.functions.FilterFunction;

public class Filter implements FilterFunction<String> {

    public boolean filter(String s) throws Exception {
        try {
            Double.parseDouble(s.trim());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
