package cloud.gump.topn

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.MyParallelSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

object ProcessAllWindowFunc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MyParallelSource).setParallelism(8).assignAscendingTimestamps(_.timestamp)
    stream
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .process(new TopNProcessFunc)
      .print().setParallelism(1)

    env.execute()
  }

}

case class EventInfo(url:String,uv:Int,rank:Int,start_time:String,end_time:String){
  override def toString: String = {
    s"${url}\t${uv}\t${rank}\t${start_time}\t${end_time}\t"
  }
}

class TopNProcessFunc extends ProcessAllWindowFunction[Event,EventInfo,TimeWindow] {
  override def process(context: Context, elements: Iterable[Event], out: Collector[EventInfo]): Unit = {
    val dateFormat = new SimpleDateFormat()
    val start_time = dateFormat.format(context.window.getStart)
    val end_time = dateFormat.format(context.window.getEnd)
    val top3List = elements
      .map(event => (event.url, 1))
      .groupBy(_._1)
      .toList
      .map(list => (list._1, list._2.size))
      .sortWith((t1, t2) => if (t1._2 > t2._2) true else false)
      .take(3)
    for(i <- top3List.indices){
      out.collect(EventInfo(top3List(i)._1, top3List(i)._2, i + 1, start_time, end_time))
    }
  }
}
