package cloud.gump.topn

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.MyParallelSource
import cloud.gump.window.TimeWindowTest.{MyPorcess, Output, UVAgg}
import org.apache.flink.api.common.functions.{AggregateFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object KeyedProcessFunc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .addSource(new MyParallelSource)
      .setParallelism(8)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .aggregate(new ViewCountAgg, new MyPorcess)
      .keyBy(_.end_time)
      .process(new MyKeyedProcessFunc)
      .print()
      .setParallelism(1)

    env.execute()
  }

  class MyKeyedProcessFunc extends KeyedProcessFunction[String,Output,String] {

    private var UVCountListState:ListState[Output] = _

    override def open(parameters: Configuration): Unit = {
      UVCountListState = getRuntimeContext.getListState(new ListStateDescriptor[Output]("list-state",classOf[Output]))
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Output, String]#OnTimerContext, out: Collector[String]): Unit = {
      println("---------timer-----------")
      val top3List = UVCountListState
        .get()
        .toList
        .sortWith(_.uv > _.uv)
        .take(3)
      UVCountListState.clear()
      for (i <- top3List.indices) {
        out.collect(top3List(i).toString)
      }
    }

    override def processElement(value: Output, ctx: KeyedProcessFunction[String, Output, String]#Context, out: Collector[String]): Unit = {
//      println(s"${value.start_time} ----- ${value.end_time}")
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      UVCountListState.add(value)
      val timer = dateFormat.parse(value.end_time).getTime + 1L
//      println(timer)
      ctx.timerService().registerEventTimeTimer(timer)
    }
  }

  class ViewCountAgg extends AggregateFunction[Event, Int, Int] {
    override def createAccumulator(): Int = 0

    override def add(value: Event, accumulator: Int): Int = accumulator + 1

    override def getResult(accumulator: Int): Int = accumulator

    override def merge(a: Int, b: Int): Int = a + b
  }}
