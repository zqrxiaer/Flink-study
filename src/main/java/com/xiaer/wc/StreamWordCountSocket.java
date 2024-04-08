package com.xiaer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 从socket中读取无界流数据，体验真实的流处理，事件驱动、高效计算、快速响应，永不停止
public class StreamWordCountSocket {
    public static void main(String[] args) throws Exception {
        // create environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // read data
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);
        // process data: split,transform,group by,sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap(
                (String value, Collector<Tuple2<String, Integer>> out)->{ // flatMap中传入的lambda表达式，java只能识别到Tuple2类型，但是无法得到Tuple2<String，Integer>
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.INT)) // java中的lambda表达式存在泛型擦除的问题，flink中自带有类型提取系统，分析函数输入和返回值类型，显示的提供类型才能工作
                .keyBy(value -> value.f0)
                .sum(1);
        // print result
        sum.print();
        // execute environment
        env.execute();
    }
}
