package chap5;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        KeyedStream<Event, String> keyedStream = dataStreamSource.keyBy(e -> e.users);

        KeyedStream<Event, String> keyedStream1 = dataStreamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.users;
            }
        });

        keyedStream.print();
        keyedStream1.print();

        env.execute();


    }

}
