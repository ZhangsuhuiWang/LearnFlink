package chap09;

import chap05.ClickSource;
import chap05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AverageTimestampExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.keyBy(r -> r.urls)
                .flatMap(new AvgTsResult())
                .print();

        env.execute();
    }

    private static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        AggregatingState<Event, Long> aggregatingState;
        ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            aggregatingState =
                    getRuntimeContext().getAggregatingState(
                            new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                                    "avg-ts",
                                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                        @Override
                                        public Tuple2<Long, Long> createAccumulator() {
                                            return Tuple2.of(0L, 0L);
                                        }

                                        @Override
                                        public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> longLongTuple2) {
                                            return Tuple2.of(longLongTuple2.f0 + event.timestamp, longLongTuple2.f1 + 1);
                                        }

                                        @Override
                                        public Long getResult(Tuple2<Long, Long> longLongTuple2) {
                                            return longLongTuple2.f0 / longLongTuple2.f1;
                                        }

                                        @Override
                                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                                            return null;
                                        }
                                    }, Types.TUPLE(Types.LONG, Types.LONG)));

            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Long.class));
        }


        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            Long count = valueState.value();
            if(count == null) {
                count = 1L;
            } else {
                count++;
            }

            valueState.update(count);
            aggregatingState.add(event);

            if(count == 5) {
                collector.collect(event.users + " 平均访问时间戳: " + new Timestamp(aggregatingState.get()));
                aggregatingState.clear();
            }
        }
    }
}
