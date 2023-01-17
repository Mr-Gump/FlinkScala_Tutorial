package cloud.gump.transformation

import cloud.gump.source_api.{MyParallelSource, MySource}
import org.apache.flink.streaming.api.scala._

object PhysicalPartition {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MySource)
    val stream1 = env.addSource(new MyParallelSource)


    // shuffle  随机分区
//    stream.shuffle.print("shuffle").setParallelism(4)

    // rebalance  轮询分区
//    stream.rebalance.print("rebalance").setParallelism(4)

    // rescale  一个上游分区对应下游分区数/上游分区数个分区，比rebalance 效率更高
//    stream1.setParallelism(2).rescale.print("rescale").setParallelism(4)

    // broadcast 一个分区中的数据复制多份到下游分区
//    stream.broadcast.print("broadcast").setParallelism(4)

    // global 强制把所有数据全部发送到第一个分区（无视下游并行度）
    stream.global.print("global").setParallelism(4)

    env.execute()
  }

}
