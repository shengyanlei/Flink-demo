package com.shyl.source;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SourceForWindow implements SourceFunction<Tuple4<String, Integer, String,Long>> {
    public static Logger LOG = LoggerFactory.getLogger(SourceForWindow.class);
    private volatile boolean isRunning = true;
    // 发送元素间隔时间
    private long sleepTime;
//    发送完一组是否休息10秒
    private boolean stopSession=false;

    public static final String[] WORDS = new String[]{
            "java",
            "java",
            "java",
            "flink",
            "flink",
            "flink",
            "hadoop",
            "hadoop",
            "hadoop",
    };

    public SourceForWindow(long sleepTime,boolean stopSession) {
        this.sleepTime = sleepTime;
        this.stopSession = stopSession;
    }

    @Override
    public void run(SourceContext<Tuple4<String, Integer, String,Long>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            String word = WORDS[count % WORDS.length];
            Long timestamp = System.currentTimeMillis();
            String time = getHHmmss(timestamp);
            Tuple4<String, Integer, String,Long> tuple2 = Tuple4.of(word, count, time,timestamp);
            ctx.collect(tuple2);
            System.out.println("send data :" + tuple2);
            if (stopSession && count==WORDS.length){
                Thread.sleep(10000);
            }else {
                Thread.sleep(sleepTime);
            }
            count++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static String getHHmmss(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        String str = sdf.format(new Date(time));
        return "时间:" + str;
    }

}
