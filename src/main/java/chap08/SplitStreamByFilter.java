package chap08;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.addSource(new ClickSource());

        DataStream<Event> maryStream = singleOutputStreamOperator.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.users.equals("Mary");
            }
        });

        DataStream<Event> bobStream = singleOutputStreamOperator.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.users.equals("Bob");
            }
        });

        DataStream<Event> otherStream = singleOutputStreamOperator.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return !event.users.equals("Mary") && !event.users.equals("Bob");
            }
        });

        maryStream.print("Mary pv");
        bobStream.print("Bob pv");
        otherStream.print("else pv");

        env.execute();
    }
}
