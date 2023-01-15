package cloud.gump.source_api

import org.apache.flink.streaming.api.scala._

object FromElem {

  case class Event(user: String, url: String, timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Event] = env.fromElements(
      Event("Alice", "/home", 1000),
      Event("Bob", "/cart", 2000),
      Event("Henry", "/order", 3000)
    )

    dataStream.print("element")

    env.execute()
  }

}
