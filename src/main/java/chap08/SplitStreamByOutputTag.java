package chap08;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStreamByOutputTag {
    private static OutputTag<Tuple3<String, String, Long>> maryStream = new OutputTag<Tuple3<String, String, Long>>("mary pv"){};
    private static OutputTag<Tuple3<String, String, Long>> bobStream = new OutputTag<Tuple3<String, String, Long>>("bob pv"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> result = singleOutputStreamOperator.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                if(event.users.equals("Mary")) {
                    context.output(maryStream, new Tuple3<>(event.users, event.urls, event.timestamp));
                } else if(event.users.equals("Bog")) {
                    context.output(bobStream, new Tuple3<>(event.users, event.urls, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });

        result.getSideOutput(maryStream).print();
        result.getSideOutput(bobStream).print();
        result.print();

        env.execute();
    }
}
