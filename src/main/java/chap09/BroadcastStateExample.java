package chap09;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Action> actionDataStreamSource = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        DataStreamSource<Pattern> patternDataStreamSource = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy")
        );

        MapStateDescriptor<Void, Pattern> mapStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastState = patternDataStreamSource.broadcast(mapStateDescriptor);


        DataStream<Tuple2<String, Pattern>> dataStreamSource = actionDataStreamSource
                .keyBy(data -> data.userId)
                .connect(broadcastState)
                .process(new PatternEvaluator());

        dataStreamSource.print();

        env.execute();

    }

    public static class Action {
        public String userId;
        public String action;

        Action() {
        }

        Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        Pattern() {
        }

        Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        ValueState<String> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastActionState", Types.STRING));
        }

        @Override
        public void processElement(Action action, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            Pattern pattern = readOnlyContext.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);
            String preAction = valueState.value();
            if(pattern != null && preAction != null) {
                if(pattern.action1.equals(preAction) && pattern.action2.equals(action.action)) {
                    collector.collect(new Tuple2<>(readOnlyContext.getCurrentKey(), pattern));
                }
                valueState.update(action.action);
            }
        }

        @Override
        public void processBroadcastElement(Pattern pattern, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            BroadcastState<Void, Pattern> broadcastState = context.getBroadcastState(new MapStateDescriptor<>("Pattern", Types.VOID, Types.POJO(Pattern.class)));
            broadcastState.put(null, pattern);
        }
    }
}
