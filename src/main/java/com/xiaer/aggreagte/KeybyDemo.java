package com.xiaer.aggreagte;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1l, 1),
                new WaterSensor("sensor_1", 4L, 4),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        KeyedStream<WaterSensor, String> keyBy = source.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
//        SingleOutputStreamOperator<WaterSensor> result = keyBy.sum("vc");
//        SingleOutputStreamOperator<WaterSensor> result = keyBy.max("vc");
        SingleOutputStreamOperator<WaterSensor> result = keyBy.maxBy("vc");
//        keyBy.
        result.print();
//        keyBy.print();
        env.execute();
    }
}
