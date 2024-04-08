package com.xiaer.transform;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.FilterFunctionImpl;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class filterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 4L, 4),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
//        SingleOutputStreamOperator<WaterSensor> filter = source.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor value) throws Exception {
//                return "sensor_1".equals(value.getId());
//            }
//        });
        SingleOutputStreamOperator filter = source.filter(new FilterFunctionImpl("sensor_1"));
        filter.print();
        env.execute();
    }
}
