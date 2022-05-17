package chap06;

import chap05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;

public class WatermarkTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.socketTextStream("local", 7777)
//                .map(new MapFunction<String, Event>() {
//                    @Override
//                    public Event map(String s) throws Exception {
//                        String[] fields = s.split(",");
//                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
//                    }
//                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event event, long l) {
//                        return event.timestamp;
//                    }
//                }))
//                .keyBy(data -> data.users)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .process(new WatermarkTestResult())
//                .print();

        env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
// 插入水位线的逻辑
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
                .keyBy(data -> data.users)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkTestResult())
                .print();

        env.execute();
    }

    private static class WatermarkTestResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long curWatermark = context.currentWatermark();
            Long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素,窗口闭合计算时,水位线处于：" + curWatermark);
        }
    }
}
