package com.xiaer.state;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class KeyedMapStateDemo {
    // 统计每个id中不同水位值vc出现的次数
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(
                        new WaterSensor("s1",1L,1),
                        new WaterSensor("s2",2L,2),
                        new WaterSensor("s1", 2L, 2),
                        new WaterSensor("s1", 3L, 3),
                        new WaterSensor("s2", 2L, 2),
                        new WaterSensor("s1", 4L, 2),
                        new WaterSensor("s1", 4L, 2),
                        new WaterSensor("s1", 3L, 1),
                        new WaterSensor("s2", 9L, 1)
                )
                .keyBy(value -> value.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            MapState<Integer, Integer> mapState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                 mapState= getRuntimeContext().getMapState(
                                        new MapStateDescriptor<>("MapState", Types.INT, Types.INT)
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                int vcKey=value.getVc();
                                if (mapState.contains(vcKey)) {
                                    mapState.put(vcKey,mapState.get(vcKey)+1);
                                }else {
                                    mapState.put(vcKey,1);
                                }
                                Iterator<Map.Entry<Integer, Integer>> iterator = mapState.iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<Integer, Integer> state = iterator.next();
                                    out.collect("id:"+value.getId()+"的结果是："+state.toString());
                                }
//                                for (Map.Entry<Integer, Integer> state : mapState.entries()) {
//                                    out.collect("id:"+value.getId()+"的结果是："+state.toString());
//                                }
                                out.collect("==================================");
                            }
                        }
                ).print();
        env.execute();
    }
}
