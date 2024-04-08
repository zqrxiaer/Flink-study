package com.xiaer.window;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, GlobalWindow> sensorWS = sensorKS
//                .countWindow(5);
                .countWindow(5,2); //滑动计数窗口，以步长（slide参数）确定窗口，而不是以窗口大小（size参数）确定窗口
        SingleOutputStreamOperator<String> result = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long maxTime = context.window().maxTimestamp();
                String windowTime = DateFormatUtils.format(maxTime, "yyyy-MM-dd hh:mm:ss.SSS");

                long count = elements.spliterator().estimateSize();
                out.collect("key=" + s + "的窗口[" + maxTime + "包含了" + count + "条数据：" + elements.toString() + "]");
            }
        });
        result.print();
        env.execute();
    }
}
