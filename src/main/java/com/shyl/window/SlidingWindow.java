package com.shyl.window;

import com.shyl.source.SourceForWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class SlidingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Tuple4<String, Integer, String, Long>> tuple4Source = env.addSource(new SourceForWindow(1000));

        WatermarkStrategy<Tuple4<String, Integer, String, Long>> watermarkStrategy = WatermarkStrategy.<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> event.f3);

        // TODO: 2024/5/23 事件时间
        tuple4Source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy("f0")
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .sum("f1")
                .print("sum");

        // TODO: 2024/5/23 处理时间
//        tuple4Source
//                .keyBy("f0")
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//                .sum("f1")
//                .print();


        env.execute();
    }
}
