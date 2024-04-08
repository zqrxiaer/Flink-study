package com.xiaer.confluence;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<Integer> elements1 = env.fromElements(1,2,3,4);
        DataStreamSource<Integer> elements2 = env.fromElements(15,16,17,18);
        DataStreamSource<Integer> elements4 = env.fromElements(25,26,27,28);
        DataStreamSource<String> elements3 = env.fromElements("5","6","7","8");

        DataStream<Integer> unionStream = elements1.union(elements2,elements4,elements3.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        }));
        unionStream.print();
        env.execute();
    }
}
