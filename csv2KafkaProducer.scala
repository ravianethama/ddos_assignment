import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
object csv2KafkaProducer extends App {
  // kafka configurations variables
  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")

  // reading csv file to a dataframe
  System.setProperty("hadoop.home.dir", "C:\\soft\\hadoop_home")
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("load CSV")
    .getOrCreate()

  val df = spark.read
    .option("header", false)
    .option("inferSchema", "true")
    .csv("C:\\Users\\anethma\\Desktop\\phData\\apache-access-log.txt")

  // kafka Producer

  val producer = new KafkaProducer[String, String](props)
  val topic = "topic_text1"
  try {

    val logFileRDD = spark.read.textFile("C:\\Users\\anethma\\Desktop\\phData\\apache-access-log.txt")
    logFileRDD.foreachPartition((aPartition: scala.collection.Iterator[String]) => {
      aPartition.foreach((line: String) => {
        try {
          val message = new ProducerRecord[String, String](topic, "partition0-key", line)
          println(line)
          producer.send(message)
        } catch {
          case ex: Exception => {
            System.err.println(ex.getMessage, ex)
          }
        }
      })
    })
  }
  catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}

