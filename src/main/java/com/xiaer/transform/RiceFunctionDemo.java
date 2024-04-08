package com.xiaer.transform;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RiceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 4L, 4),
                new WaterSensor("sensor_1", 3L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        SingleOutputStreamOperator<WaterSensor> filter = source.filter(new RichFilterFunction<WaterSensor>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(getRuntimeContext().getIndexOfThisSubtask()+"  "+getRuntimeContext().getTaskNameWithSubtasks());
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close");
            }

            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return "sensor_1".equals(value.getId());
            }
        });
        filter.print();
        env.execute();
    }
}
