package cloud.gump.transformation

import cloud.gump.source_api.FromElem.Event
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TransFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Event] = env.fromElements(
      Event("Alice", "/home", 1000),
      Event("Bob", "/cart", 2000),
      Event("Henry", "/order", 3000)
    )

    dataStream.flatMap(new MyFlatMap).print()

    env.execute()
  }

  class MyFlatMap extends FlatMapFunction[Event,String] {
    override def flatMap(value: Event, out: Collector[String]): Unit = {
      if (value.user == "Alice"){
        out.collect(value.user)
      }else{
        out.collect(value.user)
        out.collect(value.url)
      }
    }
  }
}
