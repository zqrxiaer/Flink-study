package com.xiaer.function;

import com.xiaer.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.kafka.common.protocol.types.Field;

public class FilterFunctionImpl implements FilterFunction<WaterSensor> {
    private String id;
    public FilterFunctionImpl(String id){
        this.id=id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }

}
