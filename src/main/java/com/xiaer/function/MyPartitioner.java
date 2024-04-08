package com.xiaer.function;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key)%numPartitions;
    }
}
