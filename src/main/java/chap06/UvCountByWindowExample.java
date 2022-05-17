package chap06;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UvCountByWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> singleOutputStreamOperator =
                env.addSource(new ClickSource())
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        singleOutputStreamOperator.keyBy(date -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();

        env.execute();
    }

    private static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            for(Event element: elements) {
                userSet.add(element.users);
            }
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口: " + new Timestamp(start) + "~" + new Timestamp(end) + " 的独立访客数量是：" + userSet.size());
        }
    }
}
