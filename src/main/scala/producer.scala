/*
 * Author : Saketa Chalamchala
 *
 * Creates a FileStream of XML data.
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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

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

  def main(args: Array[String]): Unit = {

    val inputFileName : String = props.getProperty("input.form.path")
    val schema : StructType = StructType(
        StructField("key", StringType, true) ::
        StructField("value", StringType, false) :: Nil)

    val ADPForm : String = Source.fromFile(inputFileName).mkString
    val ADPFormRow  = List(Row("1", ADPForm)).asJava
    val ADPFormDF = spark.createDataFrame(ADPFormRow, schema)


    ADPFormDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", props.getProperty("kafka.bootstrap.servers"))
      .option("topic", props.getProperty("kafka.topic"))
      .save()



  }
}

