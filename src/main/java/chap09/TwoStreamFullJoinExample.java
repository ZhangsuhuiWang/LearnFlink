package chap09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class TwoStreamFullJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                                return stringStringLongTuple3.f2;
                            }
                        }));

        DataStream<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                                return stringStringLongTuple3.f2;
                            }
                        }));

        stream1.keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    private ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list-state", Types.TUPLE(Types.STRING, Types.STRING)));
                        stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream2-list-state", Types.TUPLE(Types.STRING, Types.STRING)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        stream1ListState.add(stringStringLongTuple3);
                        for(Tuple3<String, String, Long> right: stream2ListState.get()) {
                            collector.collect(stringStringLongTuple3 + " => " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> stringStringLongTuple3, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        stream2ListState.add(stringStringLongTuple3);
                        for(Tuple3<String, String, Long> left: stream1ListState.get()) {
                            collector.collect(stringStringLongTuple3 + " => " + left);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
