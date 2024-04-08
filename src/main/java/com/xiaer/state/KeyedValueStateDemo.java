package com.xiaer.state;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedValueStateDemo {
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
                        new WaterSensor("s3", 9L, 90))
                .keyBy(value -> value.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            ValueState<Integer> lastVCState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                // 配置TTl
                                StateTtlConfig ttlConfig = StateTtlConfig
                                        .newBuilder(Time.seconds(10))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build();

                                // 初始化值状态 lastVCState, 不能再new一个，重新创建一个，那么初始化个鬼啊
                                lastVCState = getRuntimeContext().getState(
                                        /**
                                         *T: 状态的类型
                                         * name: 唯一的值状态描述器的命名
                                         *typeInfo: 状态的类型信息，要使用flink包中的Types类
                                         */
                                        new ValueStateDescriptor<Integer>("lastVCState", Types.INT)
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                Integer vc = value.getVc();
                                if (lastVCState.value()==null){
                                    lastVCState.update(vc);
                                }else {
                                    int lastVc=lastVCState.value();
                                    if (Math.abs(lastVc-vc)>10){
                                        out.collect("传感器=" + value.getId() + "==>当前水位值=" + vc + ",与上一条水位值=" + lastVc + ", 相差超过 10！！！！");
                                    }
                                    lastVCState.update(vc);
                                }
                            }
                        })
                .print();
        env.execute();
    }

    /*
    *
    *
    * */
}
