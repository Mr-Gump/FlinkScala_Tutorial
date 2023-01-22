package cloud.gump.window

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.{FromElem, MyParallelSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows, TumblingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

object TimeWindowTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[FromElem.Event] = env.
      addSource(new MyParallelSource).setParallelism(4).
      assignAscendingTimestamps(_.timestamp)

    stream
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .aggregate(new UVAgg, new MyPorcess)
      .print()
      .setParallelism(1)

    env.execute()
  }

  case class Output(url:String,uv:Int,start_time:String,end_time:String){
    override def toString: String = {
      s"start_time:${start_time}\tend_time:${end_time}\turl:${url}\tuv:${uv}"
    }
  }

  class UVAgg extends AggregateFunction[Event,Set[String],Int] {
    override def createAccumulator(): Set[String] = Set[String]()

    override def add(value: Event, accumulator: Set[String]): Set[String] = accumulator + value.user

    override def getResult(accumulator: Set[String]): Int = accumulator.size

    override def merge(a: Set[String], b: Set[String]): Set[String] = a ++ b
  }

  class MyPorcess extends ProcessWindowFunction[Int,Output,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Int], out: Collector[Output]): Unit = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val start_time = dateFormat.format(context.window.getStart)
      val end_time = dateFormat.format(context.window.getEnd)
      val uv = elements.iterator.next()
      out.collect(Output(key,uv,start_time,end_time))
    }
  }
}

