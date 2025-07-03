package com.dzzxjl.mammut.func;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MyKeyedProcessFunction extends KeyedProcessFunction {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // ValueState
    }

    @Override
    public void processElement(Object o, Context context, Collector collector) throws Exception {

    }
}
