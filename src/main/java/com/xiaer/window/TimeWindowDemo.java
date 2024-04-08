package com.xiaer.window;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))); //滚动处理时间窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))); //滑动处理时间窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2))); //处理时间会话窗口
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() { //动态处理时间会话窗口
                    @Override
                    public long extract(WaterSensor element) {
                        return element.getTs();
                    }
                }));
        SingleOutputStreamOperator<String> result = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd hh:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                long count = elements.spliterator().estimateSize();
                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "包含了" + count + "条数据：" + elements.toString() + "]");
            }
        });

        result.print();
        env.execute();
    }
}
