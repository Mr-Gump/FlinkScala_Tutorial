package cloud.gump.source_api

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._

import java.util.Properties

object FromKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.put("bootstrap.servers","hadoop101:9092")
    properties.put("group.id","consumer")
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset","latest")
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
    dataStream.print()
    env.execute()
  }
}
