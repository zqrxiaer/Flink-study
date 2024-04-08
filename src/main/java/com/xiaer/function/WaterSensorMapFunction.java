package com.xiaer.function;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.kafka.common.protocol.types.Field;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] data=value.split(",");
        return new WaterSensor(data[0],Long.parseLong(data[1]),Integer.parseInt(data[2]));
    }
}
