package cloud.gump.late_data

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.MyParallelSource
import cloud.gump.window.TimeWindowTest.UVAgg
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object HandleLateData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new MyParallelSource)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)) //设置最大乱序程度
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(element: Event, recordTimestamp: Long): Long = element.timestamp
      }))
      .keyBy(_.user)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))  //设置延迟等待时间
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(OutputTag[Event]("sideOutput"))  //设置侧输出流
      .aggregate(new UVAgg)
      .print("normal")

    env.execute()

  }

}
