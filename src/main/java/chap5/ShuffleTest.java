package chap5;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.addSource(new ClickSource());

        dataStreamSource.shuffle().print("shuffle").setParallelism(4);

        env.execute();
    }
}
