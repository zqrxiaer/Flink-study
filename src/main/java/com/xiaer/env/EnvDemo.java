package com.xiaer.env;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.set()
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
