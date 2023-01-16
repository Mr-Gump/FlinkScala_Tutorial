package cloud.gump.transformation

import cloud.gump.source_api.FromElem.Event
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object TransFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Event] = env.fromElements(
      Event("Alice", "/home", 1000),
      Event("Bob", "/cart", 2000),
      Event("Henry", "/order", 3000)
    )

    dataStream.filter(_.user == "Bob").print()

    dataStream.filter(new UserFilter).print()

    env.execute()
  }

  class UserFilter extends FilterFunction[Event] {
    override def filter(value: Event): Boolean = {
      value.user == "Bob"
    }
  }
}
