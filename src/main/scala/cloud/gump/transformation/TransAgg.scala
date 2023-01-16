package cloud.gump.transformation

import cloud.gump.source_api.FromElem.Event
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

object TransAgg {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream: DataStream[Event] = env.fromElements(
      Event("Alice", "/home", 1000),
      Event("Bob", "/cart", 2000),
      Event("Henry", "/order", 4000),
      Event("Henry", "/pro", 3000),
      Event("Henry", "/pro1", 5000)
    )

    val keyedStream = dataStream.keyBy(new MyKeySelector)

    // sum

    // max  只比较指定字段，其他字段和第一个元素相同
    keyedStream.max("timestamp").print()

    // min

    // maxBy 比较指定字段，获取拥有最大字段的元素本身
    keyedStream.maxBy("timestamp").print()

    // minBy

    // reduce
    dataStream.
      map(event => (event.user , 1)).
      keyBy(_._1).
      reduce((state,data)=>(state._1,state._2 + data._2)).
      keyBy(_ => true).
      reduce((state,data) => if (state._2 >= data._2) state else data).
      print()

    env.execute()
  }
  class MyKeySelector extends KeySelector[Event,String] {
    override def getKey(value: Event): String = {
      value.user
    }
  }

}

