package cloud.gump.watermark

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.{FromElem, MyParallelSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration
import java.util.concurrent.TimeUnit

object WaterMark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MyParallelSource)
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
          element.timestamp
        }
      }))
    stream.print()
    env.execute()
  }

}
