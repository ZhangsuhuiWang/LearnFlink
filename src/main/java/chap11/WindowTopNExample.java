package chap11;

import chap05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class WindowTopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> streamOperator = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Table eventTable = tableEnvironment.fromDataStream(streamOperator,
                $("users"),
                $("urls"),
                $("timestamp").rowtime().as("ts"));

        tableEnvironment.createTemporaryView("EventTable", eventTable);

        String subQuery = "select window_start, window_end, users, count(urls) as cnt " +
                "from table(tumble(Table EventTable, descriptor(ts), interval '1' hour))" +
                "group by window_start, window_end, users";

        String topNQuery = "select *" +
                "from (" +
                "select *, " +
                "row_number() over (partition by window_start, window_end " +
                "order by cnt desc" +
                ") as row_num " +
                "from (" + subQuery + "))" +
                "where row_num <= 2";

        Table result = tableEnvironment.sqlQuery(topNQuery);

        tableEnvironment.toDataStream(result).print();
        env.execute();
    }
}
