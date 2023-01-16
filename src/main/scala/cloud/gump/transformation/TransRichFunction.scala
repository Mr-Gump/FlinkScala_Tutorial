package cloud.gump.transformation

import cloud.gump.source_api.FromElem.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import java.util.Calendar

object TransRichFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val dataStream: DataStream[Event] = env.fromElements(
      Event("Alice", "/home", 1000),
      Event("Bob", "/cart", 2000),
      Event("Henry", "/order", 3000)
    )

    dataStream.map(new MyRichMap).print()

    env.execute()
  }

  class MyRichMap extends RichMapFunction[Event,(String,Long)] {
    override def map(value: Event): (String, Long) = {
      (value.user,Calendar.getInstance.getTimeInMillis)
    }

    // 每个任务开始时调用
    override def open(parameters: Configuration): Unit = {
      println(s"${getRuntimeContext.getIndexOfThisSubtask}-数据库连接初始化... ")
    }

    // 每个任务结束时调用
    override def close(): Unit = {
      println(s"${getRuntimeContext.getIndexOfThisSubtask}-关闭数据库连接...")
    }
  }

}
