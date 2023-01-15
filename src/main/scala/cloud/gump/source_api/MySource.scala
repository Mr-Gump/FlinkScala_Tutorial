package cloud.gump.source_api

import cloud.gump.source_api.FromElem.Event
import cloud.gump.source_api.MySource.flag
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.Calendar
import scala.util.Random

class MySource extends SourceFunction[Event]{

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val random = new Random()
    val users = List("alice", "bob", "henry")
    val urls = List("/home", "/order", "/cart", "/pro?id=1", "/pro?id=2")
    while (flag){
      ctx.collect(Event(users(random.nextInt(users.size)),urls(random.nextInt(urls.size)),Calendar.getInstance.getTimeInMillis))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}

object MySource{
  var flag = true
}
