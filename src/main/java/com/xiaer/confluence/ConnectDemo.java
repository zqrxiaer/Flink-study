package com.xiaer.confluence;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<Integer> elements1 = env.fromElements(1,2,3,4);
        DataStreamSource<Integer> elements2 = env.fromElements(15,16,17,18);
        DataStreamSource<String> elements3 = env.fromElements("5","6","7","8");
        DataStreamSource<Integer> elements4 = env.fromElements(25,26,27,28);
        ConnectedStreams<Integer, String> connect = elements1.connect(elements3);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来自于数字流：" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "come from String:" + value;
            }
        });
        map.print();
        env.execute();
    }
}
