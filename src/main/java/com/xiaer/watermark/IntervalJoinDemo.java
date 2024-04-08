package com.xiaer.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("a", 3),
                Tuple2.of("a", 4)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 2, 2),
                Tuple3.of("a", 3, 3),
                Tuple3.of("a", 4, 4),
                Tuple3.of("a", 5, 5),
                Tuple3.of("a", 6, 6)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(value -> value.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(value -> value.f0);
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2).between(Time.seconds(-2), Time.seconds(1)).process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
            @Override
            public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(left + "<--->" + right);
            }
        });
        process.print();
        env.execute();
    }
}
