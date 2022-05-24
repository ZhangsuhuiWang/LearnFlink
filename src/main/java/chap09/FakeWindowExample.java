package chap09;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        stream.keyBy(r -> r.urls)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    private static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        private Long windowSize;

        MapState<Long, Long> windowPvMapState;
        public FakeWindowResult(long l) {
            this.windowSize = l;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvMapState =  getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            Long start = event.timestamp / windowSize * windowSize;
            Long end = start + windowSize;
            context.timerService().registerEventTimeTimer(end - 1L);

            if(windowPvMapState.contains(start)) {
                Long count = windowPvMapState.get(start);
                windowPvMapState.put(start, count + 1);
            } else {
                windowPvMapState.put(start, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long end = timestamp + 1;
            Long start = end - windowSize;
            Long pv = windowPvMapState.get(start);
            out.collect("url: " + ctx.getCurrentKey() + " 访问量: " + pv + " 窗口: " + new Timestamp(start) + "~" + new Timestamp(end));
            windowPvMapState.remove(start);
        }
    }
}
