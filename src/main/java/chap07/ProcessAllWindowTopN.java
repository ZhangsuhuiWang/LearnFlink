package chap07;

import chap05.ClickSource;
import chap05.Event;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;


public class ProcessAllWindowTopN {
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

        SingleOutputStreamOperator<String> singleOutputStreamOperator1 = singleOutputStreamOperator.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.users;
            }
        }).windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        HashMap<String, Long> hashMap = new HashMap<>();
                        for(String url: iterable) {
                            if(hashMap.containsKey(url)) {
                                long count = hashMap.get(url);
                                hashMap.put(url, count + 1L);
                            } else {
                                hashMap.put(url, 1L);
                            }
                        }
                        ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<Tuple2<String, Long>>();
                        for(String url: hashMap.keySet()) {
                            arrayList.add(Tuple2.of(url, hashMap.get(url)));
                        }
                        arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("========================================\n");
                        for(int i = 0; i < 2; i++) {
                            Tuple2<String, Long> tuple2 = arrayList.get(i);
                            String info = "浏览量 No." + (i + 1) + " url：" + tuple2.f0 + " 浏览量：" + tuple2.f1 + "窗口结束时间:" + new TimeStamp(context.window().getEnd()) + "\n";
                            stringBuilder.append(info);
                        }
                        stringBuilder.append("========================================\n");
                        collector.collect(stringBuilder.toString());
                    }
                });


        singleOutputStreamOperator1.print();
        env.execute();
    }
}
