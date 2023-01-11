package cloud.gump.wordcount

import org.apache.flink.streaming.api.scala._

object WordCountBoundedStream {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取文本文件
    val lineDataStream: DataStream[String] = env.readTextFile("input/words.txt")

    val wordAndOne: DataStream[(String, Int)] = lineDataStream.flatMap(_.split(" ")).map(Tuple2(_, 1))

    val wordAndOneGroup: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)

    val res: DataStream[(String, Int)] = wordAndOneGroup.sum(1)

    res.print()

    // 执行任务
    env.execute()
  }

}
