package chap07;

import chap05.ClickSource;
import chap05.Event;
import chap06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;


public class KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        SingleOutputStreamOperator<UrlViewCount> singleOutputStreamOperator1 = singleOutputStreamOperator.keyBy(data -> data.urls)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        SingleOutputStreamOperator<String> result = singleOutputStreamOperator1
                .keyBy(data -> data.windowEnd)
                .process(new TopN(2));

        result.print("result");

        env.execute();
    }

    private static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            collector.collect(new UrlViewCount(s, iterable.iterator().next(), start, end));
        }
    }

    private static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private Integer n;
        private ListState<UrlViewCount> urlViewCountListState;
        public TopN(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
            urlViewCountListState.add(urlViewCount);
            context.timerService().registerEventTimeTimer(context.getCurrentKey() + 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> arrayList = new ArrayList<>();
            for(UrlViewCount urlViewCount: urlViewCountListState.get()) {
                arrayList.add(urlViewCount);
            }

            urlViewCountListState.clear();

            arrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("========================================\n");
            stringBuilder.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for(int i = 0; i < 2; i++) {
                UrlViewCount urlViewCount = arrayList.get(i);
                String info = "浏览量 No." + (i + 1) + " url：" + urlViewCount.url + " 浏览量：" + urlViewCount.count + "\n";
                stringBuilder.append(info);
            }
            stringBuilder.append("========================================\n");
            out.collect(stringBuilder.toString());
        }
    }
}
