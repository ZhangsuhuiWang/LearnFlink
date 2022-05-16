# Flink中的时间和窗口
## Flink中时间的定义
1. 处理时间(processing time):指执行处理操作的机器的系统时间.
2. 事件时间(event time):指每个事件在对应的设备上发生的事件,也就是数据生成的时间.
3. 摄入事件(ingestion time):指数据进入flink数据流时间.

## 水位线
水位线特性:
* 水位线是插入数据流中的一个标记,可以认为是一个特殊的数据.
* 水位线主要的内容是一个时间戳,用来表示当前事件时间的进展.
* 水位线是基于数据的时间戳生成的.
* 水位线的时间戳必须单调递增,以保证任务的事件时间时钟一直向前推进.
* 水位线可以通过设置延迟,来保证处理乱序数据.
* 一个水位线watermark(t),表示当前流中事件时间已经到达了时间戳t,这代表t之前的所有数据都到齐了,之后流中不会出现时间戳t' < t的数据.

.assignTimestampsAndWatermarks()方法需要传入一个WatermarkStrategy作为参数,这就是所谓的“水位线生成策略”,WatermarkStrategy中包含一个“时间戳分配器”(TimestampAssigner)和一个“水位线生成器”(WatermarkGenerator).
```Java
public interface WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T>
{
@Override
TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context);
@Override
WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
}
```
* TimestampAssigner:主要负责从流中数据元素的某个字段提取时间戳,并分配给元素.
* WatermarkGenerator:主要负责按照既定的方式,基于时间戳生成水位线,在WartermarkGenerator接口中,主要又有两个方法:onEvent()和onPeriodicEmit().
    * onEvent:每个事件(数据)到来都会调用的方法,它的参数有当前事件、时间戳,以及允许发出水位线的一个WatermarkOutput,可以基于事件做出各种操作.
    * onPeriodicEmit:周期性调用方法,可以由WatermarkOutput发出水位线.周期时间为处理时间,可以调用环境配置的.setAutoWatermarkInterval()方法来设置,默认为200ms.

### Flink内置水位线生成线
1. 有序流
    * 主要特点就是时间戳单调增长,直接调用WatermarkStrategy.forMonotonousTimestamps()方法,直接拿当前最大时间戳作为水位线就可以了.
    * 时间戳和水位线的单位,必须是毫秒.
2. 乱序流
    * 调用WatermarkStrategy.forBoundedOutOfOrderness()方法就可以实现,这个方法需要传入一个maxOutOfOrderness参数,表示“最大乱序程度”,数据流中乱序数据时间戳的最大差值.
    * 乱序流中生成的水位线真正时间戳是'当前最大时间戳-延迟时间-1'


### 自定义水位线策略
* Flink生成水位线的方式:一种是周期性的(Periodic),一种断点式的(Punctuated).
1. 周期性水位线生成器(Periodic Generator)
    * 周期性生成器一般通过onEvent()观察判断输入事件,而在onPeriodicEmit()里发出水位线.
2. 断点式水位线生成器(Punctuated Generator)
    * 断点式生成器不停地检测onEvent()重的事件,当发现带有水位线信息的特殊事件时,就立即发出水位线.

### 在自定义数据源中发送水位线
* 在自定义数据源中发送水位线之后,就不能再程序中使用assignTimestampAndWatermarks方法来生成水位线.

## 窗口(window)
### 窗口的分类
#### 按照驱动类型分类
![img.png](img.png)
1. 时间窗口
    1. Flink中有专门的累表示时间窗口,TimeWindow,这个类有两个私有属性:start和end.
2. 计数窗口
    1. Flink底层是通过全局窗口(global window)来实现的.
#### 按照窗口分配数据的规则分类.
1. 滚动窗口(Tumbling Window)
   ![img_1.png](img_1.png)
   滚动窗口有固定的大小,是一种对数据进行的“均匀切片”的划分方式.
1. 滑动窗口(Sliding Window)
   ![img_2.png](img_2.png)
   与滚动窗口的区别在于,窗口之间不是首尾相接的.定义滑动窗口的参数有两个:窗口大小(window size)和滑动步长(window slide)
1. 会话窗口(Session Window)
   ![img_3.png](img_3.png)
   基于时间定义.
3. 全局窗口(Global Window)
   ![img_4.png](img_4.png)

### 窗口API概览
#### 按键分区(Keyed)和非按键分区(Non-Keyed)
1. 按键分区窗口(Keyed Windows)
    1. 数据流会按照key分为多条逻辑流,这就是KeyedStream.
2. 非按键分区(Non-Keyed Windows)
    1. 窗口逻辑只能在一个task上执行,相当于并行度为1
#### 代码中窗口API调用
```Java
stream.keyBy(<key selector>)
      .window(<window assigner>)
      .aggregate(<winodw function>)
```
窗口操作只要有两部分:窗口分配器(Window Assigners)和窗口函数(Window Function)

### 窗口分配器(Window Assigners)
#### 时间窗口
1. 滚动处理时间窗口
    * .of()重载方法可以传入两个Time类型参数:size和offset,size是窗口大小,offset是窗口起始点的偏移量.
```Java
stream.keyBy(...)
      .window(TumblingProcessingTimeWindows.of(Time.second(5)))
      .aggregate(...)
```
1. 滑动处理时间窗口
    * .of()方法传入两个Time类型参数:size和slide,size表示滑动窗口大小,后者表示滑动窗口的滑动步长.同样可以追加第三个参数,用于指定窗口起始点的偏移量.
```Java
stream.keyBy(...)
      .window()
      .aggregate(SlidingProcessingTimeWindows.of(Time.seconds(10), Time
      .seconds(5)))
```
1. 处理时间会话窗口
    * .withGap()方法需要传入一个Time类型的参数size,表示会话的超时时间.
    * .withDynamicGap()方法需要传入一个SessionWindowTimeGapExtractor作为参数.
```Java
stream.keyBy(...)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .aggregate(...)
```
```Java
    .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
        @Override
        public long extract(Tuple2<String, Long> element) {
        // 提取 session gap 值返回, 单位毫秒
            return element.f0.length() * 1000;
        }
    }))
```
1. 滚动事件时间窗口
```Java
stream.keyBy(...)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(...)
```
1. 滑动事件时间窗口
```Java
stream.keyBy(...)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(...)
```
1. 事件时间会话窗口
```Java
stream.keyBy(...)
      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .aggregate(...)
```

#### 计数窗口
1. 滚动计数窗口
```Java
stream.keyBy(...)
      .countWindow(10)
```
1. 滑动计数窗口
```Java
stream.keyBy(...)
      .countWindow(10，3)
```

#### 全局窗口
* 全局窗口需要自行定义触发器才能实现窗口计算.
```Java
stream.keyBy(...)
      .window(GlobalWindows.create());
```

### 窗口函数(Window Functions)
![img_5.png](img_5.png)
* 窗口函数根据处理方式可以分为两类:增量聚合函数和全窗口函数.
#### 增量聚合函数(incremental aggregation functions)
1. 归约函数(ReduceFunction)
2. 聚合函数(AggregateFunction)
    1. Flink的window API中的aggregate提供了这样的操作,直接基于WindowedStream调用.aggregate()方法.
    2. 接口中的四个方法:
        * createAccumulator():创建一个累加器,为聚合创建一个初始状态,每个聚合任务只会调用一次.
        * add():将输入的元素添加到累加器中.
        * getResult():从累加器中提取聚合的输出结果.
        * merge():合并两个累加器.
```Java
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable
{
    ACC createAccumulator();
    ACC add(IN value, ACC accumulator);
    OUT getResult(ACC accumulator);
    ACC merge(ACC a, ACC b);
}
```
#### 全窗口函数(full window functions)