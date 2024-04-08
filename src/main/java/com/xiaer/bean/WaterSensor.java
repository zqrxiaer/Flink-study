package com.xiaer.bean;

import java.util.Objects;

public class WaterSensor {
    public String id;
    public Long ts;
    public Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return id.equals(that.id) && ts.equals(that.ts) && vc.equals(that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }

    @Override
    public String toString() { //可以自定义打印对象时的输出格式
        return "WaterSensor:{id="+this.getId()+",ts="+this.getTs()+",vc="+this.getVc()+"}";
    }
}
