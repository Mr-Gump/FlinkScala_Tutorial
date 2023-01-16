package cloud.gump.transformation

import cloud.gump.source_api.FromElem.Event
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object TransMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Event] = env.fromElements(
      Event("Alice", "/home", 1000),
      Event("Bob", "/cart", 2000),
      Event("Henry", "/order", 3000)
    )

    val stream1 = dataStream.map(_.user)
    stream1.print()

    val stream2 = dataStream.map(new UserExtraction)
    stream2.print()

    env.execute()
  }

  class UserExtraction extends MapFunction[Event,String] {
    override def map(value: Event): String = {
      value.user
    }
  }

}
