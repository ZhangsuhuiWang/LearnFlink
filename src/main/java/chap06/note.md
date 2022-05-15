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