package org.github.wajda.aracot

import com.arangodb.async.ArangoDatabaseAsync
import org.apache.commons.lang.StringUtils.substringBefore
import org.slf4s.Logging
import za.co.absa.spline.common.AsyncCallRetryer
import za.co.absa.spline.persistence.ArangoImplicits.ArangoDatabaseAsyncScalaWrapper
import za.co.absa.spline.persistence.RetryableExceptionUtils
import za.co.absa.spline.persistence.model.DataSource

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, Future}
import scala.util.{Failure, Success}

class Controller(
  db: ArangoDatabaseAsync,
  iterations: Int,
  maxRetries: Int,
  avgDelayMs: Int,
  ignoreRevs: Boolean,
  resetToZero: Boolean,
) extends Logging {

  log.debug(s"Start controller: iterations=$iterations, maxRetries=$maxRetries, avgDelayMs=$avgDelayMs, ignoreRevs=$ignoreRevs")

  import scala.concurrent.ExecutionContext.Implicits.global

  private val cnt = AtomicPersistentCounter.retryable("tx", db, maxRetries, ignoreRevs)

  if (resetToZero || !Await.result(cnt.checkInitialized(), Duration.Inf)) {
    log.debug(s"Resetting counter to 0")
    Await.result(cnt.reset(), Duration.Inf)
    log.debug(s"Counter reset done")
  }

  private val nStarted = new AtomicInteger(0)
  private val nFailed = new AtomicInteger(0)
  private val nDone = new AtomicInteger(0)

  def action(): Awaitable[_] = {
    log.debug(s"Start concurrent actions")

    (1 to iterations)
      .map(_ => {
        if (avgDelayMs > 0) {
          Thread.sleep((math.random * avgDelayMs * 2).toLong + 1)
        }

        val nAct = nStarted.incrementAndGet()

        val t0 = System.nanoTime()
        val eventualRetVal = tick1()

        log.debug(s"action #$nAct\t: started")

        eventualRetVal.onComplete({
          case Failure(e) => synchronized {
            nFailed.incrementAndGet()
            val t1 = System.nanoTime()
            val dt = t1 - t0

            log.debug(s"action #$nAct\t: failed after ${dt / 1000000} ms\tError: ${substringBefore(e.getCause.getMessage, "\n")}")
            log.trace("", e)
          }
          case Success(retVal) => synchronized {
            nDone.incrementAndGet()
            val t1 = System.nanoTime()
            val dt = t1 - t0
            log.debug(s"action #$nAct\t: done in ${dt / 1000000} ms\tRetVal: $retVal")
          }
        })
        eventualRetVal.map(_ => ())
      })
      .foldLeft(Future.successful(())) {
        case (acc, f) =>
          val f2 = acc.flatMap(_ => f)
          f2.recoverWith {
            case _ => f.flatMap(_ => f2).recoverWith { case _ => f2 }
          }
      }
  }

  def logStats(): Unit = synchronized {
    val ns = nStarted.get()
    val nf = nFailed.get()
    val nd = nDone.get()

    log.info(s"Actions started   : $ns")
    log.info(s"Actions finished  : ${nf + nd} ($nf failed, $nd succeeded)")
    log.info(s"Actions pending   : ${ns - nd - nf}")
  }

  private def tick1(): Future[_] = cnt.next()

  // ################################################################################################

  private val retryer2 = new AsyncCallRetryer(RetryableExceptionUtils.isRetryable, 20)

  private def tick2(): Future[_] = retryer2.execute {
    val dataSources: Array[DataSource] = (1 to 20).map(i => DataSource(s"ds$i", s"ds$i", null)).toArray
    db.queryAs[AnyRef](
      s"""
         |WITH dataSource
         |FOR doc IN @dataSources
         |  INSERT UNSET(doc, ['_key']) INTO dataSource
         |""".stripMargin,
      Map("dataSources" -> dataSources)
    )
  }
}
