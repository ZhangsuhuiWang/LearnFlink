package chap12;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class NFAExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<LoginEvent, String> keyedStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        )
                .keyBy(r -> r.userId);

        DataStream<String> dataStream = keyedStream.flatMap(new StateMachineMapper());
        dataStream.print("warning");

        env.execute();
    }

    @SuppressWarnings("serial")
    private static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {
        private ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent loginEvent, Collector<String> collector) throws Exception {
            State state = currentState.value();
            if(state == null) {
                state = State.Initial;
            }

            State nextState = state.transition(loginEvent.eventType);

            if(nextState == State.Matched) {
                collector.collect(loginEvent.userId + "连续三次登录失败");
            } else if(nextState == State.Terminal) {
                currentState.update(state.Initial);
            } else {
                currentState.update(nextState);
            }
        }
    }
}




