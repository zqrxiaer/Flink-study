package com.xiaer.window;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(20)));
        SingleOutputStreamOperator<String> result = sensorWS.aggregate(new MyAgg(), new MyProcss());
        result.print();
        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String>{

        @Override
        public Integer createAccumulator() {
            System.out.println("create accumulator");
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("call add method,value="+value);
            return accumulator+value.getVc();
        }

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("call getResult method");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("call merge method");
            return null;
        }
    }

    public static class MyProcss extends ProcessWindowFunction<String,String,String,TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd hh:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = elements.spliterator().estimateSize();
            out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "包含了" + count + "条数据：" + elements.toString() + "]");
        }
    }
}

/*
s1,1,1
s1,2,2
s1,3,3
s1,4,4
s1,5,5
s1,6,6
s1,7,7
s2,8,8
s2,9,9
s2,10,10
s3,11,11
s3,12,12
 */
