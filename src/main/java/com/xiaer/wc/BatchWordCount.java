package com.xiaer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //TODO 1.配置可执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.读取word.txt文件，按行读取，存储的元素就是每行的文本
        DataSource<String> lineDS = env.readTextFile("input/word.txt");
        //TODO 3.转换数据格式，转换为（word，1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        //TODO 4.按照相同的word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne.groupBy(0);
        //TODO 5.统计聚合分组内单词出现次数
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);
        //TODO 6.打印结果
        sum.print();
    }
}
