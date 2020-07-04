package com.vesoft.nebula.tools

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import com.typesafe.config.Config
import java.io.File
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.google.common.base.Optional
import com.google.common.geometry.{S2CellId, S2LatLng}
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors, RateLimiter}
import com.vesoft.nebula.client.graph.async.AsyncGraphClientImpl
import com.vesoft.nebula.graph.ErrorCode
import com.vesoft.nebula.tools.generator.v2.reader.Neo4JReader
import com.vesoft.nebula.tools.generator.v2.{Configs, DataSourceConfigEntry, FileBaseSourceConfigEntry, HiveSourceConfigEntry, KafkaSourceConfigEntry, Neo4jSourceConfigEntry, SocketSourceConfigEntry, SourceCategory}
import org.apache.spark.SparkConf
//import com.vesoft.nebula.tools.generator.v2.reader.{Neo4JReader,Reader, ServerBaseReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

final case class Argument(
                           config: File = new File("application.conf"),
                           hive: Boolean = false,
                           directly: Boolean = false,
                           dry: Boolean = false,
                           reload: String = ""
                         )

object Neo4jSparkImporterTest {

  private[this] val LOG = Logger.getLogger(this.getClass)

  private[this] val BATCH_INSERT_TEMPLATE                           = "INSERT %s %s(%s) VALUES %s"
  private[this] val INSERT_VALUE_TEMPLATE                           = "%s: (%s)"
  private[this] val INSERT_VALUE_TEMPLATE_WITH_POLICY               = "%s(\"%s\"): (%s)"
  private[this] val ENDPOINT_TEMPLATE                               = "%s(\"%s\")"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE             = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE                             = "%s->%s@%d: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE_WITH_POLICY                 = "%s->%s@%d: (%s)"
  private[this] val USE_TEMPLATE                                    = "USE %s"

  private[this] val DEFAULT_EDGE_RANKING = 0L
  private[this] val DEFAULT_ERROR_TIMES  = 16

  private[this] val NEWLINE = "\n"

  // GEO default config
  private[this] val DEFAULT_MIN_CELL_LEVEL = 10
  private[this] val DEFAULT_MAX_CELL_LEVEL = 18

  private[this] val MAX_CORES = 64

  private object Type extends Enumeration {
    type Type = Value
    val VERTEX = Value("VERTEX")
    val EDGE   = Value("EDGE")
  }

  private object KeyPolicy extends Enumeration {
    type POLICY = Value
    val HASH = Value("hash")
    val UUID = Value("uuid")
  }

  def main(args: Array[String]): Unit = {
    val PROGRAM_NAME = "Spark Writer"
    val parser = new scopt.OptionParser[Argument](PROGRAM_NAME) {
      head(PROGRAM_NAME, "1.0.0")

      opt[File]('c', "config")
        .required()
        .valueName("<file>")
        .action((x, c) => c.copy(config = x))
        .text("config file")

      opt[Unit]('h', "hive")
        .action((_, c) => c.copy(hive = true))
        .text("hive supported")

      opt[Unit]('d', "directly")
        .action((_, c) => c.copy(directly = true))
        .text("directly mode")

      opt[Unit]('D', "dry")
        .action((_, c) => c.copy(dry = true))
        .text("dry run")

      opt[String]('r', "reload")
        .valueName("<path>")
        .action((x, c) => c.copy(reload = x))
        .text("reload path")
    }

    val c: Argument = parser.parse(args, Argument()) match {
      case Some(config) => config
      case _ =>
        LOG.error("Argument parse failed")
        sys.exit(-1)
    }

    val configs = Configs.parse(c.config)
    LOG.info(s"Config ${configs}")

//    val fs = FileSystem.get(new Configuration())
//    val hdfsPath = new Path(configs.errorConfig.errorPath)
//    try {
//      if (!fs.exists(hdfsPath)) {
//        LOG.info(s"Create HDFS directory: ${configs.errorConfig.errorPath}")
//        fs.mkdirs(hdfsPath)
//      }
//    } finally {
//      fs.close()
//    }

    val sessionBuild = SparkSession
      .builder()
      .appName(PROGRAM_NAME)

    if (configs.tagsConfig.nonEmpty) {
      val neo4jSourceConfigEntry = configs.tagsConfig.head.dataSourceConfigEntry.asInstanceOf[Neo4jSourceConfigEntry]
      val sparkConf : SparkConf = new SparkConf().setAppName("InitSpark").setMaster("local[*]")
      sparkConf.set("spark.neo4j.bolt.url", neo4jSourceConfigEntry.address)
      sparkConf.set("spark.neo4j.bolt.user",neo4jSourceConfigEntry.user)
      sparkConf.set("spark.neo4j.bolt.password",neo4jSourceConfigEntry.password)
      sessionBuild.config(sparkConf)
      val session = sessionBuild.getOrCreate()
      session.sparkContext.setLogLevel("WARN")
      val neo4jReader = new Neo4JReader(session, neo4jSourceConfigEntry.address, neo4jSourceConfigEntry.user,
        Option(neo4jSourceConfigEntry.password),
        true, "")
      println("===============================")
      for(tagConfig <- configs.tagsConfig){
        val query_smt = s"""match (n:${tagConfig.label})
          return ${if(tagConfig.vertexField!="id()") s"n.${tagConfig.vertexField}" else "id(n)"}
          ${val t=tagConfig.fields.keys.filter(_!=tagConfig.vertexField).map("n."+_).mkString(",")
          if(t.isEmpty) "" else s",${t}" }""".split('\n').map(_.trim).mkString(" ")
        var more = true
        var count = 0L
        while (more){
          val dataFrame = neo4jReader.read(query_smt+" SKIP "+count+"+$_skip LIMIT $_limit")
          count+=dataFrame.count()
          if(dataFrame.count()<tagConfig.batch)
            more=false
          dataFrame.show()

        }
      }
    }
  }
}
