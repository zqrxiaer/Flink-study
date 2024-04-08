package com.xiaer.partition;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setParallelism(2);
        DataStreamSource<WaterSensor> sensor = en.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 4L, 4),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
//        sensor.shuffle().print();
//        sensor.rebalance().print();
//        sensor.rescale().print();
//        sensor.broadcast().print();
        sensor.global().print();
        en.execute();

    }
}
