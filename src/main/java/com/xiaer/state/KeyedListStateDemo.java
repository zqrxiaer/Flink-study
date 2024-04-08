package com.xiaer.state;

import com.xiaer.bean.WaterSensor;
import org.apache.commons.compress.archivers.dump.DumpArchiveEntry;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;

public class KeyedListStateDemo {
    // 任务：输出水位值（VC）排名前三的数据

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(new WaterSensor("s1",1L,1),
                        new WaterSensor("s2",2L,2),
                        new WaterSensor("s1", 2L, 2),
                        new WaterSensor("s1", 3L, 13),
                        new WaterSensor("s2", 2L, 22),
                        new WaterSensor("s1", 4L, 4),
                        new WaterSensor("s1", 3L, 3),
                        new WaterSensor("s2", 9L, 90)
                )
                .keyBy(value -> value.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            ListState<Integer> listStates;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                listStates= getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                listStates.add(value.getVc());
                                Iterable<Integer> vcIt = listStates.get();
//                                listStates.
                                List<Integer> vcList = new ArrayList<>();
                                for (Integer vc : vcIt) {
                                    vcList.add(vc);
                                }
                                //方案一：找出最小的哪个，删除最小的
                                for (Integer vc : vcList) {
                                    if (value.getVc()>vc) {

                                    }
                                }

                                //方案二：重新排序，把最后一个去掉
//                                vcList.sort((o1, o2) -> o2-o1);
//                                if (vcList.size()>3) {
//                                    vcList.remove(3);
//                                }
                                out.collect("ID为"+value.getId()+"的数据中，水位值前三的是"+vcList.toString());
                                listStates.update(vcList);
                            }
                        }
                )
                .print();
        env.execute();
    }
}
