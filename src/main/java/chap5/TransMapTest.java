package chap5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        dataStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.users;
            }
        });

        dataStream.map(new UserExtractor()).print();

        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.users;
        }
    }

}
