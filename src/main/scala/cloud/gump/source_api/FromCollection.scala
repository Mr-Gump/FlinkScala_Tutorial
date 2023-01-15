package cloud.gump.source_api

import cloud.gump.source_api.FromElem.Event
import org.apache.flink.streaming.api.scala._

object FromCollection {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.
      fromCollection(List(Event("Alice", "/home", 1000), Event("Bob", "/cart", 2000), Event("Henry", "/order", 3000)))
      .print("collection")
    env.execute()
  }

}
