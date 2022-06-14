package chap11;


import chap05.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableToStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.createTemporaryView("EventTable", singleOutputStreamOperator);

        Table aliceVisitTable = tableEnvironment.sqlQuery("select users, urls from EventTable where users = 'Alice'");

        Table urlCountTable = tableEnvironment.sqlQuery("select users, count(urls) from EventTable group by users");

        tableEnvironment.toDataStream(aliceVisitTable).print("Alice visit");
        tableEnvironment.toChangelogStream(urlCountTable).print("count");

        env.execute();


    }
}
