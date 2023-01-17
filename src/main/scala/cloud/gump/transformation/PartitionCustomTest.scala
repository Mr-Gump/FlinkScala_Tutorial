package cloud.gump.transformation

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.MySource
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object PartitionCustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MySource)
    stream.partitionCustom(new MyPartitioner,_.timestamp).print("Custom").setParallelism(2)
    env.execute()
  }

  class MyPartitioner extends Partitioner[Long] {
    override def partition(key: Long, numPartitions: Int): Int = {
      (key % 2).toInt
    }
  }

}
