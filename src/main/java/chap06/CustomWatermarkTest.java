package chap06;

import chap5.ClickSource;
import chap5.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();
        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp;
                }
            };
        }

        @Override
        public WatermarkStrategy<Event> withTimestampAssigner(TimestampAssignerSupplier<Event> timestampAssigner) {
            return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
        }

        @Override
        public WatermarkStrategy<Event> withTimestampAssigner(SerializableTimestampAssigner<Event> timestampAssigner) {
            return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
        }

        @Override
        public WatermarkStrategy<Event> withIdleness(Duration idleTimeout) {
            return WatermarkStrategy.super.withIdleness(idleTimeout);
        }

        private class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
            private Long delayTime = 5000L;
            private Long maxTs = Long.MAX_VALUE + delayTime + 1L;
            @Override
            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                maxTs = Math.max(event.timestamp, maxTs);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
            }
        }
    }



}
