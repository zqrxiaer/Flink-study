package com.xiaer.window;

import com.xiaer.bean.WaterSensor;
import com.xiaer.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorDS
                .keyBy(r -> r.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<String> result = sensorWS.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器");
                return 0;
            }

            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("调用add方法，value="+value);
                return accumulator + value.getVc();
            }

            @Override
            public String getResult(Integer accumulator) {
                System.out.println("调用getresult方法");
                return accumulator.toString();
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("调用merge方法");
                return null;
            }
        });
        result.print();
        env.execute();
    }
}
