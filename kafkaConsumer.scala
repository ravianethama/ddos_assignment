import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.collection.mutable.HashMap

object kafkaConsumer extends App {
  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("topic_text1")
  var hashMapVar = new HashMap[String,String]()
  var ipList:List[String] = List()
    try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())
        val ipAddress = record.value.toString.split(" - - ".toArray)
        //print ip address : a(0)
        println(ipAddress(0))
        // regular expression to find the date timestamp
        val datePattern = new Regex("[0-9]{2}/[A-Z][a-z]{2}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}")
        val dateVal = (datePattern findAllIn record.value.toString).mkString("")
        // print date timestamp
        println(dateVal)

        // generate key value pair : key -> date time and value -> ip address
        if(hashMapVar.contains(dateVal))
          {ipList=ipList :+ ipAddress(0)}
          else
          {hashMapVar.update(dateVal,ipAddress(0))}
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
      // Print list of DDOS ip address
      println(" List of DDOS ip's")
      ipList.distinct.foreach(System.out.println)
      System.out.println("DONE")
  }


}
