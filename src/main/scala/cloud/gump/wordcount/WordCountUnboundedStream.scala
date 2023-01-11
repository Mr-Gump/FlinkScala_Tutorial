package cloud.gump.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCountUnboundedStream {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取参数
    val param = ParameterTool.fromArgs(args)
    val host = param.get("host")
    val port = param.getInt("port")

    // 监听端口

    val lineDataStream: DataStream[String] = env.socketTextStream(host,port)

    val wordAndOne: DataStream[(String, Int)] = lineDataStream.flatMap(_.split(" ")).map(Tuple2(_, 1))

    val wordAndOneGroup: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)

    val res: DataStream[(String, Int)] = wordAndOneGroup.sum(1)

    res.print()

    // 执行任务
    env.execute()
  }

}
