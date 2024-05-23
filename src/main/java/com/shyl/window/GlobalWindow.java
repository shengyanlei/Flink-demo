package com.shyl.window;

import com.shyl.source.SourceForWindow;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;

public class GlobalWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple4<String, Integer, String, Long>> tuple4Source = env.addSource(new SourceForWindow(1000, false));
        env.setParallelism(2);
        // TODO: 2024/5/23 基于元素数量的全局窗口
        tuple4Source.keyBy("f0")
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(3))
                .sum("f1")
                .print("global window sum");
    }
}
