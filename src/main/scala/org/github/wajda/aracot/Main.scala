package org.github.wajda.aracot

import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.Logger.ROOT_LOGGER_NAME
import org.slf4j.LoggerFactory
import scopt.OptionParser
import za.co.absa.spline.common.security.TLSUtils
import za.co.absa.spline.persistence.{ArangoConnectionURL, ArangoDatabaseFacade}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends App {
  implicit class OptionParserOps(val p: OptionParser[Config]) extends AnyVal {
    def placeNewLine(): Unit = p.note("")
  }

  private val cliParser: OptionParser[Config] = new OptionParser[Config]("ARACOT (ArangoDB Atomic Counter Tester)") {
    head(
      s"""
         |ARACOT (ArangoDB Atomic Counter Tester)
         |Version: ${AracotBuildInfo.Version} (rev. ${AracotBuildInfo.Revision})
         |""".stripMargin
    )

    help("help").text("Print this usage text.")
    version('v', "version").text("Print version info.")

    {
      val logLevels = classOf[Level].getFields.collect { case f if f.getType == f.getDeclaringClass => f.getName }
      val logLevelsString = logLevels.mkString(", ")

      opt[String]('l', "log-level")
        .text(s"Log level ($logLevelsString). Default is ${Config().logLevel}.")
        .validate(l => if (logLevels.contains(l.toUpperCase)) success else failure(s"<log-level> should be one of: $logLevelsString"))
        .action((str, conf) => conf.copy(logLevel = Level.valueOf(str)))
    }

    opt[Int]('n', "iterations")
      .text(s"Number of increments. Default is ${Config().iterations}.")
      .validate(n => if (n > 0) success else failure("<iterations> should be a positive integer"))
      .action((n, conf) => conf.copy(iterations = n))

    opt[Int]('r', "retries")
      .text(s"Max number of retries in case of a collision. Default is ${Config().maxRetries}.")
      .validate(n => if (n > -1) success else failure("<retries> should be a non-negative integer"))
      .action((n, conf) => conf.copy(maxRetries = n))

    opt[Int]('d', "delay")
      .text(s"Avg operation delay in millis. The actual value is random, 0 <= x <= delay*2. Default is ${Config().avgDelayMs}.")
      .validate(n => if (n > -1) success else failure("<delay> should be a non-negative integer"))
      .action((n, conf) => conf.copy(avgDelayMs = n))

    opt[Unit]("ignore-revs")
      .text(s"Ignore revs. Default is ${Config().ignoreRevs}.")
      .action((_, conf) => conf.copy(ignoreRevs = true))

    arg[String]("<db_url>")
      .required()
      .text(s"ArangoDB connection string in the format: ${ArangoConnectionURL.HumanReadableFormat}")
      .action { case (url, c) => c.copy(connectionUrl = Some(ArangoConnectionURL(url))) }

    checkConfig {
      case Config(None, _, _, _, _, _) =>
        failure("No Connection URL is given")
      case _ =>
        success
    }
  }

  cliParser.parse(args, Config()) match {
    case Some(Config(Some(connUrl), logLevel, iterations, maxRetries, avgDelayMs, ignoreRevs)) =>
      LoggerFactory
        .getLogger(ROOT_LOGGER_NAME)
        .asInstanceOf[Logger]
        .setLevel(logLevel)

      val dbFacade = new ArangoDatabaseFacade(connUrl, Some(TLSUtils.TrustingAllSSLContext), false)

      System.setProperty("spline.database.connectionUrl", connUrl.asString)

      try {
        val t0 = System.currentTimeMillis()
        val ctrl = new Controller(dbFacade.db, iterations, maxRetries, avgDelayMs, ignoreRevs)
        Await.result(ctrl.action(), Duration.Inf)
        val t1 = System.currentTimeMillis()
        println(s"Done in ${t1 - t0} ms.")
      } finally {
        dbFacade.destroy()
      }

    case _ =>
      System.exit(1)
  }

}
