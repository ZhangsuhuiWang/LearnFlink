package chap05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFunctionUDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        DataStream<Event> dataStream = dataStreamSource.filter(new FlinkFilter());

        dataStream.print();

        env.execute();
    }


    private static class FlinkFilter implements org.apache.flink.api.common.functions.FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.urls.contains("./home");
        }
    }
}
