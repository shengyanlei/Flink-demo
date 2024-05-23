package com.shyl.window;

import com.shyl.source.SourceForWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Tuple4<String, Integer, String,Long>> tupelSource = env.addSource(new SourceForWindow(1000));

        //配置水印生成器
        WatermarkStrategy<Tuple4<String,Integer,String,Long>> watermarkStrategy = WatermarkStrategy
                .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> event.f3);
    // TODO: 2024/5/23 取f3为事件时间
        tupelSource.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy("f0")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("f1")
                .print("sum:");
        // TODO: 2024/5/23 使用处理时间
//        tupelSource
//                .keyBy("f0")
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .max("f1")
//                .print("maxdata:");
        env.execute();
    }
}
