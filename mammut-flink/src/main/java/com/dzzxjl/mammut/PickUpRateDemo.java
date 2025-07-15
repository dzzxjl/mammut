package com.dzzxjl.mammut;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Random;

public class PickUpRateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // <时间戳, 用户id, 品牌id, 行为类型>

        DataStream<Tuple4<Long, Integer, Integer, Integer>> ds = env.addSource(new SourceFunction<Tuple4<Long, Integer, Integer, Integer>>() {
            Random random = new Random();

            @Override
            public void run(SourceContext<Tuple4<Long, Integer, Integer, Integer>> ctx) throws Exception {

                while (true) {

                    long ts = System.currentTimeMillis();
                    int userId = random.nextInt(10);
                    int brandId = random.nextInt(10);
                    int actionType = random.nextInt(2);

                    ctx.collect(new Tuple4<>(ts, userId, brandId, actionType));

                    Thread.sleep(1000);
                }

            }

            @Override
            public void cancel() {

            }
        });


        // ds.keyBy(new KeySelector<Tuple4<Long, Integer, Integer, Integer>, Integer>() {
        //
        //     @Override
        //     public Integer getKey(Tuple4<Long, Integer, Integer, Integer> value) throws Exception {
        //         // 根据品牌划分
        //         return value._3();
        //     }
        // }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
        //                 .process(new ProcessWindowFunction<Tuple4<Long, Integer, Integer, Integer>, Object, Integer, TimeWindow>() {
        //                     @Override
        //                     public void process(Integer integer, ProcessWindowFunction<Tuple4<Long, Integer, Integer, Integer>, Object, Integer, TimeWindow>.Context context, Iterable<Tuple4<Long, Integer, Integer, Integer>> iterable, Collector<Object> collector) throws Exception {
        //
        //                     }
        //                 })


        ds.map(new MapFunction<Tuple4<Long, Integer, Integer, Integer>, Integer>() {
            @Override
            public Integer map(Tuple4<Long, Integer, Integer, Integer> v) throws Exception {
                System.out.println(v);
                return null;
            }
        });

        env.execute("test-job");

    }

}
