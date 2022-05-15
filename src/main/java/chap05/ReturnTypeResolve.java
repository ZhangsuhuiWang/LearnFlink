package chap05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReturnTypeResolve {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 转换成二元元祖
        // 1.使用显式的 ".returns(...)"
        DataStream<Tuple2<String, Long>> dataStream = dataStreamSource.map(event -> {
            return Tuple2.of(event.users, 1L);
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        dataStream.print();

        //2. 使用匿名类代替lambda表达式
        DataStream<Tuple2<String, Long>> dataStream1 = dataStreamSource.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.users, 1L);
            }
        });

        dataStream1.print();

        //3.使用类来代替lambda表达式
        DataStream<Tuple2<String, Long>> dataStream2 = dataStreamSource.map(new MyTuple2Map());

        dataStream2.print();

        env.execute();
    }

    private static class MyTuple2Map implements MapFunction<Event, Tuple2<String, Long>>{

        @Override
        public Tuple2<String, Long> map(Event event) throws Exception {
            return Tuple2.of(event.users, 1L);
        }
    }
}
