package com.xiaer.confluence;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<Tuple2<Integer, String>> elements1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "aa1"),
                Tuple2.of(2, "a2"),
                Tuple2.of(3, "a3")
                );
        DataStreamSource<Tuple3<Integer, String, String>> elements2 = env.fromElements(
                Tuple3.of(1, "aaa", "b"),
                Tuple3.of(1, "bbb", "ccc"),
                Tuple3.of(2, "dd", "dd"),
                Tuple3.of(1, "ee", "e")
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, String>> connect = elements1.connect(elements2);
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, String>> connetcKeyBy = connect.keyBy(new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        }, new KeySelector<Tuple3<Integer, String, String>, Integer>() {
            @Override
            public Integer getKey(Tuple3<Integer, String, String> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<String> process = connetcKeyBy.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, String>() {
            Map<Integer, List<Tuple2<Integer, String>>> cache1 = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, String>>> cache2 = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer key = value.f0;
                if (!cache1.containsKey(key)) {
                    ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
                    list1.add(value);
                    cache1.put(key, list1);
                } else {
                    cache1.get(key).add(value);
                }
                if (cache2.containsKey(key)) {
                    for (Tuple3<Integer, String, String> e : cache2.get(key)) {
                        out.collect("stream1:" + value + "---stream2" + e);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer key = value.f0;
                if (!cache2.containsKey(key)) {
                    ArrayList<Tuple3<Integer, String, String>> list2 = new ArrayList<>();
                    list2.add(value);
                    cache2.put(key, list2);
                } else {
                    cache2.get(key).add(value);
                }
                if (cache1.containsKey(key)) {
                    for (Tuple2<Integer, String> e : cache1.get(key)) {
                        out.collect("stream1:" + e + "---stream2" + value);
                    }
                }
            }
        });
        process.print();
        env.execute();
    }
}
