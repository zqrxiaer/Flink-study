package com.xiaer.process;

//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimerProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env
                .fromElements(
                        new WaterSensor("s1", 1L, 1),
                        new WaterSensor("s1", 2L, 2),
                        new WaterSensor("s1", 3L, 3),
                        new WaterSensor("s2", 2L, 2),
                        new WaterSensor("s1", 4L, 4),
                        new WaterSensor("s3", 3L, 3),
                        new WaterSensor("s3", 9L, 9)
                );
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L);
        SingleOutputStreamOperator<WaterSensor> sensorWS = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = sensorWS.keyBy(value -> value.getId());
        SingleOutputStreamOperator<String> process = sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                Long eventTS = ctx.timestamp();
                String currentKey = ctx.getCurrentKey();
                TimerService timerService = ctx.timerService();
                timerService.registerEventTimeTimer(5000L);
                System.out.println("current key:" + currentKey + ";register a 5 second event time timer");
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                String currentKey = ctx.getCurrentKey();
                System.out.println("current key:" + currentKey + "; call timer");
            }
        });
        process.print();
        env.execute();
    }
}
