package com.xiaer.transform;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 4L, 4),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        SingleOutputStreamOperator<String> flatMap = source.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("sensor_1".equals(value.getId())) {
                    out.collect(value.getTs().toString());
                } else if ("sensor_2".equals(value.getId())) {
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        });
        flatMap.print();
        env.execute();
    }
}
