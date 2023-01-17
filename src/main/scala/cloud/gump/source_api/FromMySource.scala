package cloud.gump.source_api

import org.apache.flink.streaming.api.scala._

object FromMySource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new MyParallelSource).print()
    env.execute()
  }

}
