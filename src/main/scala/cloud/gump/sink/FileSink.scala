package cloud.gump.sink

import cloud.gump.source_api.MyParallelSource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new MyParallelSource).setParallelism(2)
    val fileSink = StreamingFileSink
      .forRowFormat(
        new Path("output"),
        new SimpleStringEncoder[String]("UTF-8")
      ).withRollingPolicy(
      DefaultRollingPolicy.builder()
        .withRolloverInterval(TimeUnit.SECONDS.toSeconds(20))
        .withMaxPartSize(1024 * 1024 * 1024)
        .withInactivityInterval(TimeUnit.SECONDS.toSeconds(10))
        .build()
    ).build()

    dataStream.map(_.toString).addSink( fileSink ).setParallelism(1)

    env.execute()
  }
}
