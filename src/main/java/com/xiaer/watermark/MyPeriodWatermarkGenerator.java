package com.xiaer.watermark;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPeriodWatermarkGenerator implements WatermarkGenerator<WaterSensor> {

    private long maxTs;
    private long delayTs;

    public MyPeriodWatermarkGenerator(long delayTs){
        this.delayTs=delayTs;
        this.maxTs=Long.MIN_VALUE+this.delayTs+1;
    }

    @Override
    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
        maxTs=Math.max(eventTimestamp,maxTs);
        System.out.println("call onEvent");
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(this.maxTs-this.delayTs-1));
        System.out.println("call onPeriodicEmit");
    }
}
