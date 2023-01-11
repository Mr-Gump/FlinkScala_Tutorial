package cloud.gump.wordcount

// 隐式转换
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

/**
  * Copyright (c) 2022-2035 gump.cloud All Rights Reserved 
  *
  * Package: cloud.gump.wordcount
  * Version: 1.0
  *
  * Created by mrgump on 2023/1/11
  */
object WordCountBatch {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 读取文本文件数据
    val lineDataSet: DataSet[String] = env.readTextFile("input/words.txt")

    // 对数据集进行转换处理
    val wordAndOne: DataSet[(String, Int)] = lineDataSet.flatMap(line => line.split(" ")).map(word => Tuple2(word, 1))

    // 按照单词分组
    val wordAndOneGroup: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)

    // 对数据进行sum统计
    val res: AggregateDataSet[(String, Int)] = wordAndOneGroup.sum(1)

    // 将结果输出
    res.print()
  }
}
