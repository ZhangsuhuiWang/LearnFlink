package chap07;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.addSource(new ClickSource());
        
        singleOutputStreamOperator
                .keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, Object>() {
                            @Override
                            public void processElement(Event event, KeyedProcessFunction<Boolean, Event, Object>.Context context, Collector<Object> collector) throws Exception {
                                Long currentTs = context.timerService().currentProcessingTime();
                                collector.collect("数据到达，到达时间：" + new Timestamp(currentTs));
                                context.timerService().registerProcessingTimeTimer(currentTs + 10 * 1000L);
                            }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                })
                .print();
        
        env.execute();
    }
}
