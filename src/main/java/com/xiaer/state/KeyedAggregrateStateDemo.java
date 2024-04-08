package com.xiaer.state;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedAggregrateStateDemo {
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
                            AggregatingState<Integer,Double> aggState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                aggState=getRuntimeContext().getAggregatingState(
                                        new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>(
                                                "AggState",
                                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                    @Override
                                                    public Tuple2<Integer, Integer> createAccumulator() {
                                                        return Tuple2.of(0,0);
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                        return Tuple2.of(accumulator.f0+value,accumulator.f1+1);
                                                    }

                                                    @Override
                                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                        return accumulator.f0*1D/accumulator.f1;
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                        return null;
                                                    }
                                                },
                                                Types.TUPLE(Types.INT,Types.INT)
                                        )
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                aggState.add(value.getVc());
                                out.collect("id:"+value.getId()+",avg:"+aggState.get());
                            }

                        }
                ).print();
        env.execute();
    }
}
