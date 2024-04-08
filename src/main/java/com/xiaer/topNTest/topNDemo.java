package com.xiaer.topNTest;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class topNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(
                        new WaterSensor("s1", 1L, 1),
                        new WaterSensor("s1", 2L, 2),
                        new WaterSensor("s1", 3L, 3),
                        new WaterSensor("s1", 2L, 2),
                        new WaterSensor("s1", 4L, 4),
                        new WaterSensor("s1", 3L, 3),
                        new WaterSensor("s1", 7L, 3),
                        new WaterSensor("s1", 9L, 3),
                        new WaterSensor("s1", 13L, 5),
                        new WaterSensor("s1", 15L, 5),
                        new WaterSensor("s1", 17L, 7),
                        new WaterSensor("s1", 19L, 7),
                        new WaterSensor("s1", 23L, 7),
                        new WaterSensor("s1", 25L, 9)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) //Duration表示了3s持续时间对象
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs()*1000L)
                ).windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))) //Time是Flink中的时间对象
                .process(new myTopNFunction())
                .print();
        env.execute();
    }

    public static class myTopNFunction extends ProcessAllWindowFunction<WaterSensor,String, TimeWindow>{

        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            Map<Integer, Integer> vcCountMap = new HashMap<>();


            //TODO 1. 遍历数据
            for (WaterSensor element : elements) {
                Integer vc = element.getVc();
                //包含key，则不是第一条数据，直接累加,不包含key，是第一条数据，则直接存储
                if (vcCountMap.containsKey(vc)) {
                    vcCountMap.put(vc,vcCountMap.get(vc)+1);
                }else {
                    vcCountMap.put(vc,1);
                }
            }
            //TODO 2. 排序
            List<Tuple2<Integer, Integer>> vcList = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                vcList.add(Tuple2.of(vc,vcCountMap.get(vc)));
            }
            vcList.sort((o1, o2) -> o2.f1-o1.f1);
            //TODO 3. 取出topN
            StringBuilder outString = new StringBuilder();
            for (int i = 0; i < Math.min(vcList.size(),2); i++) {
                outString.append("top"+i);
                outString.append("\n");
                outString.append("vc="+vcList.get(i).f0+";count="+vcList.get(i).f1);
                outString.append("\n");
                outString.append("windows is ["+ DateFormatUtils.format(context.window().getStart(),"yyyy-MM-dd HH:mm:ss.SSS")+","+ DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd HH:mm:ss.SSS")+")");
                outString.append("\n");
                outString.append("===============================");
                outString.append("\n");
            }
            out.collect(outString.toString());
        }
    }
}
