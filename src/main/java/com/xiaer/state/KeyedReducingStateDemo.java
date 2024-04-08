package com.xiaer.state;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class KeyedReducingStateDemo {
    // 统计每个id中水位值的和

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(
                        new WaterSensor("s1", 1L, 16),
                        new WaterSensor("s1", 2L, 2),
                        new WaterSensor("s1", 3L, 13),
                        new WaterSensor("s2", 2L, 22),
                        new WaterSensor("s1", 4L, 4),
                        new WaterSensor("s3", 3L, 3),
                        new WaterSensor("s3", 9L, 90)
                )
                .keyBy(value -> value.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            ReducingState<Integer> reducingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                reducingState=getRuntimeContext().getReducingState(
                                        new ReducingStateDescriptor<>("ReducingState", (value1, value2) -> value1*value2,Types.INT)
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                reducingState.add(value.getVc());
                                out.collect("id:"+value.getId()+"sum(vc):"+reducingState.get());
                            }
                        }
                ).print();
        env.execute();
    }
}
