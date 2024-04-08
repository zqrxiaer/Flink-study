package com.xiaer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建可执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.读取文件
        DataStreamSource<String> lineDS = env.readTextFile("input\\word.txt");
        //TODO 3.处理数据 切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneStream = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOneStream.sum(1);
        //TODO 4.输出数据
        sum.print();
        //TODO 5.执行环境
        env.execute();
    }

}
