package com.vesoft.nebula.tools

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import com.typesafe.config.Config
import java.io.File
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.google.common.base.Optional
import com.google.common.geometry.{S2CellId, S2LatLng}
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.{
  FutureCallback,
  Futures,
  ListenableFuture,
  MoreExecutors,
  RateLimiter
}
import com.vesoft.nebula.client.graph.async.AsyncGraphClientImpl
import com.vesoft.nebula.graph.ErrorCode
import com.vesoft.nebula.spark.tools.TooManyErrorsException
import com.vesoft.nebula.tools.generator.v2.reader.Neo4JReader
import com.vesoft.nebula.tools.generator.v2.{
  Configs,
  DataSourceConfigEntry,
  FileBaseSourceConfigEntry,
  HiveSourceConfigEntry,
  KafkaSourceConfigEntry,
  Neo4jSourceConfigEntry,
  SocketSourceConfigEntry,
  SourceCategory
}
import org.apache.spark.SparkConf
//import com.vesoft.nebula.tools.generator.v2.reader.{Neo4JReader,Reader, ServerBaseReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import com.vesoft.nebula.tools.generator.v2.entry.KeyPolicy

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

  private[this] def isSuccessfully(code: Int) = code == ErrorCode.SUCCEEDED

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

    val neo4jSourceConfigEntry =
      configs.tagsConfig.head.dataSourceConfigEntry.asInstanceOf[Neo4jSourceConfigEntry]
    val sparkConf: SparkConf = new SparkConf().setAppName("InitSpark").setMaster("local[*]")
    sparkConf.set("spark.neo4j.bolt.url", neo4jSourceConfigEntry.address)
    sparkConf.set("spark.neo4j.bolt.user", neo4jSourceConfigEntry.user)
    sparkConf.set("spark.neo4j.bolt.password", neo4jSourceConfigEntry.password)
    sessionBuild.config(sparkConf)
    val session = sessionBuild.getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    val errorBuffer = ArrayBuffer[String]()

    val neo4jReader = new Neo4JReader(session,
                                      neo4jSourceConfigEntry.address,
                                      neo4jSourceConfigEntry.user,
                                      Option(neo4jSourceConfigEntry.password),
                                      true,
                                      "")
//    if (configs.tagsConfig.nonEmpty) {
    if (false) {
      println("===============================")
      for (tagConfig <- configs.tagsConfig) {

        val nebulaProperties = tagConfig.fields.values.map(_.unwrapped).mkString(",")
        println(nebulaProperties)

        val query_smt = s"""match (n:${tagConfig.label})
          return ${if (tagConfig.vertexField != "id()")
          s"n.${tagConfig.vertexField} as ${tagConfig.vertexField}"
        else "id(n)"}
          ${
          val t = tagConfig.fields.keys
            .filter(_ != tagConfig.vertexField)
            .map(v => "n." + v + " as " + v)
            .mkString(",")
          if (t.isEmpty) "" else s",${t}"
        }""".split('\n').map(_.trim).mkString(" ")
        var more  = true
        var count = 0L
        while (more) {
          val dataFrame = neo4jReader.read(query_smt + " SKIP " + count + "+$_skip LIMIT $_limit")
          count += dataFrame.count()
          if (dataFrame.count() < tagConfig.batch)
            more = false
          dataFrame
            .map(row => {
              val neo4jValues = (for {
                neo4jProperty <- tagConfig.fields.keys.toList
                if neo4jProperty.trim.length != 0
              } yield extraValue(row, neo4jProperty))
              (extraValue(
                 row,
                 if (tagConfig.vertexField != "id()") tagConfig.vertexField else "id(n)").toString,
               neo4jValues.mkString(","))
            })(Encoders.tuple(Encoders.STRING, Encoders.STRING))
            .foreachPartition { iterator: Iterator[(String, String)] =>
              {
                val service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
                val hostAndPorts =
                  configs.databaseConfig.addresses.map(HostAndPort.fromString).asJava
                val client = new AsyncGraphClientImpl(
                  hostAndPorts,
                  configs.connectionConfig.timeout,
                  configs.connectionConfig.retry,
                  configs.executionConfig.retry
                )
                client.setUser(configs.userConfig.user)
                client.setPassword(configs.userConfig.password)

                if (isSuccessfully(client.connect())) {
                  val errorBuffer = ArrayBuffer[String]()
                  val switchSpaceCode =
                    client.execute(USE_TEMPLATE.format(configs.databaseConfig.space)).get().get()
                  if (isSuccessfully(switchSpaceCode)) {
                    val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
                    val rateLimiter = RateLimiter.create(configs.rateConfig.limit)
                    iterator.grouped(tagConfig.batch).foreach { tags =>
                      val exec = BATCH_INSERT_TEMPLATE.format(
                        Type.VERTEX.toString,
                        tagConfig.name,
                        nebulaProperties,
                        tags
                          .map { tag =>
                            if (tagConfig.vertexPolicy.isEmpty) {
                              INSERT_VALUE_TEMPLATE.format(tag._1, tag._2)
                            } else {
                              tagConfig.vertexPolicy.get match {
                                case KeyPolicy.HASH =>
                                  INSERT_VALUE_TEMPLATE_WITH_POLICY
                                    .format(KeyPolicy.HASH.toString,
                                            tag._1.replace("\"", ""),
                                            tag._2)
                                case KeyPolicy.UUID =>
                                  INSERT_VALUE_TEMPLATE_WITH_POLICY
                                    .format(KeyPolicy.UUID.toString,
                                            tag._1.replace("\"", ""),
                                            tag._2)
                                case _ => throw new IllegalArgumentException
                              }
                            }
                          }
                          .mkString(",")
                      )

                      LOG.warn(s"Exec : ${exec}")
                      if (rateLimiter
                            .tryAcquire(configs.rateConfig.timeout, TimeUnit.MILLISECONDS)) {
                        val future = client.execute(exec)
                        futures += future
                      } else {
                        errorBuffer += exec
                        if (errorBuffer.size == configs.errorConfig.errorMaxSize) {
                          throw TooManyErrorsException(
                            s"Too Many Errors ${configs.errorConfig.errorMaxSize}")
                        }
                      }

                      val latch = new CountDownLatch(futures.size)
                      for (future <- futures) {
                        Futures.addCallback(
                          future,
                          new FutureCallback[Optional[Integer]] {
                            override def onSuccess(result: Optional[Integer]): Unit = {
                              latch.countDown()
                            }

                            override def onFailure(t: Throwable): Unit = {
                              latch.countDown()
                            }
                          },
                          service
                        )
                      }
                    }
                  }
                }

              }
            }
        }
      }
    }

    if (configs.edgesConfig.nonEmpty) {


      println("===============================")
      for (edgesConfig <- configs.edgesConfig) {
        val query_smt = s"match (s)-[r:${edgesConfig.label}]->(t) return " +
          s"${if (edgesConfig.sourceField.getOrElse("id()") != "id()")
            s"s.${edgesConfig.sourceField.mkString(",")}"
          else "id(s)"}," +
          s"${if (edgesConfig.targetField != "id()")
            "t." + edgesConfig.targetField
          else "id(t)"}," +
          s"${edgesConfig.fields.keys.map(r => "r." + r + " as " + r).mkString(",")}"
        println(query_smt)
        val nebulaValues = edgesConfig.fields.keys.mkString(",")
        var batch_count  = 0
        val batch_data   = ArrayBuffer[String]()
        val dataFrame    = neo4jReader.read(query_smt)
        dataFrame.collect()
          .map(
            row => {
              val neo4jValues = for {
                neo4jProperty <- edgesConfig.fields.keys.toList
                if neo4jProperty.trim.length != 0
              } yield extraValue(row, neo4jProperty).toString
              if(neo4jValues.size==2) println(neo4jValues+"\t"+row)
              (extraValue(row,
                          if (edgesConfig.sourceField.getOrElse("id()") != "id()")
                            "s."+edgesConfig.sourceField.mkString(",")
                          else "id(s)").toString,
               extraValue(row,
                          if (edgesConfig.targetField != "id()") "t."+edgesConfig.targetField
                          else "id(t)").toString,
               neo4jValues.mkString(","))
            })//(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING))
          .foreach(data => {
            println(data)
            val sourceVid = if (edgesConfig.sourcePolicy.isDefined) {

              edgesConfig.sourcePolicy.get match {
                case KeyPolicy.HASH =>
                  ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, data._1.replace("\"", ""))
                case KeyPolicy.UUID =>
                  ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, data._1.replace("\"", ""))
                case _ =>
                  throw new IllegalArgumentException()
              }
            } else data._1

            val targetVid = if (edgesConfig.targetPolicy.isDefined) {
              edgesConfig.targetPolicy.get match {

                case KeyPolicy.HASH =>
                  ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, data._2.replace("\"", ""))
                case KeyPolicy.UUID =>
                  ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, data._2.replace("\"", ""))
                case _ =>
                  throw new IllegalArgumentException()
              }
            } else data._2
            batch_data += EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY.format(sourceVid, targetVid, data._3)
            batch_count += 1
            if (batch_count >= edgesConfig.batch) {
              val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
              val rateLimiter = RateLimiter.create(configs.rateConfig.limit)
              val exec = BATCH_INSERT_TEMPLATE.format(Type.EDGE.toString,
                                                      edgesConfig.name,
                                                      nebulaValues,
                                                      batch_data.mkString(","))
              println(exec)
              val hostAndPorts =
                configs.databaseConfig.addresses.map(HostAndPort.fromString).asJava
              val client = new AsyncGraphClientImpl(
                hostAndPorts,
                configs.connectionConfig.timeout,
                configs.connectionConfig.retry,
                configs.executionConfig.retry
              )
              client.setUser(configs.userConfig.user)
              client.setPassword(configs.userConfig.password)

              if (isSuccessfully(client.connect())) {
                val switchSpaceCode =
                  client.execute(USE_TEMPLATE.format(configs.databaseConfig.space)).get().get()
                if (!isSuccessfully(switchSpaceCode)) {}
              }
              if (rateLimiter.tryAcquire(configs.rateConfig.timeout, TimeUnit.MILLISECONDS)) {
                val future = client.execute(exec)
                futures += future
              } else {
                LOG.debug("Save the error execution sentence into buffer")
                errorBuffer += exec
                if (errorBuffer.size == configs.errorConfig.errorMaxSize) {
                  throw TooManyErrorsException(
                    s"Too Many Errors ${configs.errorConfig.errorMaxSize}")
                }
              }
              batch_data.clear()
              batch_count = 0
            }
          }) //)


        if(batch_data.nonEmpty){

          val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
          val rateLimiter = RateLimiter.create(configs.rateConfig.limit)
          val exec = BATCH_INSERT_TEMPLATE.format(Type.EDGE.toString,
            edgesConfig.name,
            nebulaValues,
            batch_data.mkString(","))
          println(exec)
          val hostAndPorts =
            configs.databaseConfig.addresses.map(HostAndPort.fromString).asJava
          val client = new AsyncGraphClientImpl(
            hostAndPorts,
            configs.connectionConfig.timeout,
            configs.connectionConfig.retry,
            configs.executionConfig.retry
          )
          client.setUser(configs.userConfig.user)
          client.setPassword(configs.userConfig.password)

          if (isSuccessfully(client.connect())) {
            val switchSpaceCode =
              client.execute(USE_TEMPLATE.format(configs.databaseConfig.space)).get().get()
            if (!isSuccessfully(switchSpaceCode)) {}
          }
          if (rateLimiter.tryAcquire(configs.rateConfig.timeout, TimeUnit.MILLISECONDS)) {
            val future = client.execute(exec)
            futures += future
          } else {
            LOG.debug("Save the error execution sentence into buffer")
            errorBuffer += exec
            if (errorBuffer.size == configs.errorConfig.errorMaxSize) {
              throw TooManyErrorsException(
                s"Too Many Errors ${configs.errorConfig.errorMaxSize}")
            }
          }
        }

      }
    }
    session.close()
  }

  /**
   * Extra value from the row by field name.
   * When the field is null, we will fill it with default value.
   *
   * @param row   The row value.
   * @param field The field name.
   * @return
   */
  private[this] def extraValue(row: Row, field: String): Any = {
    val index = row.schema.fieldIndex(field)
    row.schema.fields(index).dataType match {
      case StringType =>
        if (!row.isNullAt(index)) {
          row.getString(index).mkString("\"", "", "\"")
        } else {
          "\"\""
        }
      case ShortType =>
        if (!row.isNullAt(index)) {
          row.getShort(index).toString
        } else {
          "0"
        }
      case IntegerType =>
        if (!row.isNullAt(index)) {
          row.getInt(index).toString
        } else {
          "0"
        }
      case LongType =>
        if (!row.isNullAt(index)) {
          row.getLong(index).toString
        } else {
          "0"
        }
      case FloatType =>
        if (!row.isNullAt(index)) {
          row.getFloat(index).toString
        } else {
          "0.0"
        }
      case DoubleType =>
        if (!row.isNullAt(index)) {
          row.getDouble(index).toString
        } else {
          "0.0"
        }
      case _: DecimalType =>
        if (!row.isNullAt(index)) {
          row.getDecimal(index).toString
        } else {
          "0.0"
        }
      case BooleanType =>
        if (!row.isNullAt(index)) {
          row.getBoolean(index).toString
        } else {
          "false"
        }
      case TimestampType =>
        if (!row.isNullAt(index)) {
          row.getTimestamp(index).getTime
        } else {
          "0"
        }
      case _: DateType =>
        if (!row.isNullAt(index)) {
          row.getDate(index).toString
        } else {
          "\"\""
        }
      case _: ArrayType =>
        if (!row.isNullAt(index)) {
          row.getSeq(index).mkString("\"[", ",", "]\"")
        } else {
          "\"[]\""
        }
      case _: MapType =>
        if (!row.isNullAt(index)) {
          row.getMap(index).mkString("\"{", ",", "}\"")
        } else {
          "\"{}\""
        }
    }
  }
}
