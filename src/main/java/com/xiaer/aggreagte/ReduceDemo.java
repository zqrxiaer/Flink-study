package com.xiaer.aggreagte;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1l, 1),
                new WaterSensor("sensor_1", 5L, 4),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 7L, 8),
                new WaterSensor("sensor_3", 3L, 3)
        );
        KeyedStream<WaterSensor, String> keyBy = source.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        SingleOutputStreamOperator<WaterSensor> reduce = keyBy.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println(value1+" "+value2);
                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
            }
        });
        reduce.print();
        env.execute();
    }
}
