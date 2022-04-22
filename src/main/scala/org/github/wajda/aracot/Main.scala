package org.github.wajda.aracot

import ch.qos.logback.classic.{Level, Logger}
import com.arangodb.async.ArangoDatabaseAsync
import org.slf4j.Logger.ROOT_LOGGER_NAME
import org.slf4j.LoggerFactory
import org.slf4s.Logging
import scopt.OptionParser
import za.co.absa.spline.common.security.TLSUtils
import za.co.absa.spline.persistence.{ArangoConnectionURL, ArangoDatabaseFacade}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, blocking}

object Main extends App with Logging {
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

    opt[Unit]("zero")
      .text(s"Reset counter to 0 before starting.")
      .action((_, conf) => conf.copy(resetToZero = true))

    opt[Unit]("ignore-revs")
      .text(s"Ignore revisions. May cause write skew!")
      .action((_, conf) => conf.copy(ignoreRevs = true))

    arg[String]("<db_url>")
      .required()
      .text(s"ArangoDB connection string in the format: ${ArangoConnectionURL.HumanReadableFormat}")
      .action { case (url, c) => c.copy(connectionUrl = Some(ArangoConnectionURL(url))) }

    checkConfig {
      case Config(None, _, _, _, _, _, _) =>
        failure("No Connection URL is given")
      case _ =>
        success
    }
  }

  cliParser.parse(args, Config()) match {
    case Some(Config(Some(connUrl), logLevel, iterations, maxRetries, avgDelayMs, ignoreRevs, resetToZero)) =>
      LoggerFactory
        .getLogger(ROOT_LOGGER_NAME)
        .asInstanceOf[Logger]
        .setLevel(logLevel)

      val dbFacade = new ArangoDatabaseFacade(connUrl, Some(TLSUtils.TrustingAllSSLContext), false)

      log.debug(s"Connected to database ${dbFacade.db.dbName}")
      System.setProperty("spline.database.connectionUrl", connUrl.asString)

      var ctrl: Controller = null
      val t0 = System.currentTimeMillis()
      try {
        initCounter(dbFacade.db)
        ctrl = new Controller(dbFacade.db, iterations, maxRetries, avgDelayMs, ignoreRevs, resetToZero)
        Await.ready(ctrl.action(), Duration.Inf)
      } finally {
        val t1 = System.currentTimeMillis()
        log.info(s"Finished in ${t1 - t0} ms.")
        if (ctrl != null) ctrl.logStats()
        log.debug("Shutting down database driver")
        dbFacade.destroy()
        log.debug("Database driver shut down")
      }

    case _ =>
      System.exit(1)
  }

  private def initCounter(db: ArangoDatabaseAsync): Unit = blocking {
    if (!db.exists().get()) db.create().get()
    val c = db.collection("counter")
    if (!c.exists().get()) c.create().get()
  }

}
