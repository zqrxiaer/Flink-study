package com.xiaer.watermark;

import com.xiaer.function.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.time.Duration;

public class WatermarkIdelnessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); //env
        env
                .setParallelism(2)
                .socketTextStream("hadoop102",7777) //input stream data
                .partitionCustom(new MyPartitioner(), new KeySelector<String, String>() { //partitioner
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                .map(new MapFunction<String, Integer>() { //map
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy // assign watermark
                        .<Integer>forMonotonousTimestamps() // Monotonous watermark strategy
                        .withTimestampAssigner(new SerializableTimestampAssigner<Integer>() { //watermark timestamp
                            @Override
                            public long extractTimestamp(Integer element, long recordTimestamp) {
                                return element*1000L;
                            }
                        })
                        .withIdleness(Duration.ofSeconds(5))) //idleness
                .keyBy(new KeySelector<Integer, Integer>() { //keyby
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value%2;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //select window
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() { //process
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(),"yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key:"+integer+",window:["+windowStart+","+windowEnd+"),contain "+ count +" records,values ==> "+elements);
                    }
                })
                .print(); //print
        env.execute();
    }
}
/**
 * flink 编程流程
 * 1. 创建环境：env
 * 2. 源算子：
 */
