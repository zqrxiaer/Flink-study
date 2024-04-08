package com.xiaer.partition;

import com.xiaer.function.MyPartitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);
        DataStreamSource<String> elements = env.fromElements("1", "2", "3", "4", "5", "6");
        DataStream<String> custom = elements.partitionCustom(new MyPartitioner(), r->r);
        custom.print("hello");
        env.execute();
    }
}
