package chap06;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }))
                .keyBy(r -> r.urls)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .process(new WindowResult())
                .print();

        env.execute();
    }

    private static class MyTrigger extends Trigger<Event, TimeWindow> {
        @Override
        public TriggerResult onElement(Event event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> valueState = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN));
            if(valueState.value() == null) {
                for(long i = timeWindow.getStart(); i < timeWindow.getEnd(); i = i + 1000L) {
                    triggerContext.registerEventTimeTimer(i);
                }
                valueState.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> valueState = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN));
            valueState.clear();
        }
    }

    private static class WindowResult extends ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(s, elements.spliterator().getExactSizeIfKnown(), context.window().getStart(), context.window().getEnd()));
        }
    }
}
