package com.shyl.window;

import com.shyl.source.SourceForWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class SessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple4<String, Integer, String, Long>> tuple4Source = env.addSource(new SourceForWindow(1000, true));
        WatermarkStrategy<Tuple4<String, Integer, String, Long>> watermarkStrategy = WatermarkStrategy
                .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, tiemstamp) -> event.f3);
        //         TODO: 2024/5/23 静态会话间隙，事件时间
//        tuple4Source.assignTimestampsAndWatermarks(watermarkStrategy)
//                .keyBy("f0")
//                .window(EventTimeSessionWindows.withGap(Time.seconds(3)))
//                .sum("f1")
//                .print("EventTIme static sum");
//         TODO: 2024/5/23 静态会话间隙，处理时间
//        tuple4Source
//                .keyBy("f0")
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(8)))
//                .sum("f1")
//                .print("ProcessingTime static sum");
        // TODO: 2024/5/23 动态会话间隔 ，事件时间(不常用，且感觉场景不多)
//        tuple4Source.assignTimestampsAndWatermarks(watermarkStrategy)
//                .keyBy("f0")
//                        .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple4<String, Integer, String, Long>>() {
//                            @Override
//                            public long extract(Tuple4<String, Integer, String, Long> element) {
//                                if (element.f1 == 0){
//                                    return 1000; //注意毫秒
//                                }else {
//                                    return element.f1*1000;
//                                }
//
//                            }
//                        }))
//                .sum("f1")
//                .print("EventTime dynamic sum");

        // TODO: 2024/5/23 动态时间间隔  处理时间(不常用，且感觉场景不多)
        tuple4Source
                .keyBy("f0")
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple4<String, Integer, String, Long>>() {
                            @Override
                            public long extract(Tuple4<String, Integer, String, Long> element) {
                                if (element.f1 == 0){
                                    return 1000; //注意毫秒
                                }else {
                                    return element.f1*1000;
                                }
                            }
                        }))
                .sum("f1")
                .print("processingTime dynamic sum");



        env.execute();
    }
}
