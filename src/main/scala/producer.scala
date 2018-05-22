/*
 * Author : Saketa Chalamchala
 *
 * Creates a Batch of XML data from local files.
 * Publishes the XML as a single message on a Secure Kafka topic .
 *
 *
 * @args :
 * -Djava.security.auth.login.config=/home/kafka-user/kafka-jaas.conf \
 * -Djava.security.krb5.conf=/etc/krb5.conf \
 * -Djavax.security.auth.useSubjectCredsOnly=false \
 * --files job.properties#job.properties,SampleADPFullForm.xml#SampleADPFullForm.xml, \
 *  kafka.jaas#kafka.jaas
 */

import java.util.Properties
import java.io.{File, FileInputStream}

import scala.io.Source
import scala.collection.JavaConverters._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object producer {

  private val props : Properties = getProperties

  private val spark = SparkSession
    .builder()
    .appName(props.getProperty("spark.app.name"))
    .getOrCreate()

  private def  getProperties : Properties = {

    val propsFileName : String = SparkFiles.get("job.properties")
    val propsFile : File = new File(propsFileName)
    val prop : Properties = new Properties()

    prop.load(new FileInputStream(propsFile))

    return prop

  }


  /* Publishes message from an input stream to Kafka topic
   * @args :
   * dataDF - key-value Batch DataFrame to be publish
   * topic - Kafka destination
   */
  def publish_batch( dataDF : DataFrame, topic : String): Unit =
  {
     dataDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
       .write
       .format("kafka")
       .option("kafka.bootstrap.servers", props.getProperty("kafka.bootstrap.servers"))
       .option("topic", topic)
       .save()
  }

  /* Publishes messages to Kafka topic in batch
   * @args :
   * dataDF - key-value Batch DataFrame to be publish
   * topic - Kafka destination
   */
  def publish_stream( dataDF : DataFrame, topic : String): Unit =
  {
    dataDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props.getProperty("kafka.bootstrap.servers"))
      .option("topic", topic)
      .start()
  }

  /* Generating input data from sample XML files
   * @args :
   *
   */
  def generate_input_stream() : DataFrame = {

    /* Reading a sample XML file  */
    val inputFileName : String = props.getProperty("input.form.path")
    val schema : StructType = StructType(
      StructField("key", StringType, true) ::
        StructField("value", StringType, false) :: Nil)

    /* Creating a DF from XML file  */
    val inputForm : String = Source.fromFile(inputFileName).mkString
    val inputRow  = List(Row("1", inputForm)).asJava
    val inputDF = spark.createDataFrame(inputRow, schema)
    return inputDF
  }

  def main(args: Array[String]): Unit = {

    /* Generate input data/stream  */
    val ADPFormDF = generate_input_stream

    /* Producing to a Kafka topic  */
    publish_batch(ADPFormDF, props.getProperty("kafka.topic"))

  }
}

