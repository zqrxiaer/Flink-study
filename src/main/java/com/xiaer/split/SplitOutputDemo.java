package com.xiaer.split;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> elements = env.fromElements(
                "s1,1,1",
                "s2,2,2",
                "s3,3,3",
                "s4,4,4",
                "s5,5,5",
                "s6,6,6"
        );
        SingleOutputStreamOperator<WaterSensor> map = elements.map(new WaterSensorMapFunction());
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    ctx.output(s1, value);
                } else if ("s2".equals(id)) {
                    ctx.output(s2, value);
                } else {
                    out.collect(value);
                }
            }
        });
        process.print("主流");
        SideOutputDataStream<WaterSensor> sideOutput1 = process.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> sideOutput2 = process.getSideOutput(s2);
        sideOutput1.print("s1");
        sideOutput2.print("s2");
        env.execute();
    }
}
