package org.github.wajda.aracot

import com.arangodb.async.ArangoDatabaseAsync
import org.apache.commons.lang.StringUtils.substringBefore
import org.slf4s.Logging
import za.co.absa.spline.common.AsyncCallRetryer
import za.co.absa.spline.persistence.model.Counter

import java.util.concurrent.atomic.AtomicInteger
import scala.compat.java8.FutureConverters.CompletionStageOps
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
  private val nFailure = new AtomicInteger(0)
  private val nSuccess = new AtomicInteger(0)

  def action(): Awaitable[_] = {
    log.debug(s"Start concurrent actions")

    (1 to iterations)
      .map(_ => {
        if (avgDelayMs > 0) {
          Thread.sleep((math.random * avgDelayMs * 2).toLong + 1)
        }

        val nAct = nStarted.incrementAndGet()

        val t0 = System.nanoTime()
        val eventualRetVal = cnt.next()

        log.debug(s"action #$nAct\t: started")

        eventualRetVal.onComplete({
          case Failure(e) => synchronized {
            nFailure.incrementAndGet()
            val t1 = System.nanoTime()
            val dt = t1 - t0

            log.debug(s"action #$nAct\t: failed after ${dt / 1000000} ms\tError: ${substringBefore(e.getCause.getMessage, "\n")}")
            log.trace("", e)
          }
          case Success(retVal) => synchronized {
            nSuccess.incrementAndGet()
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
      .transformWith(_ => new AsyncCallRetryer(_ => true, Int.MaxValue).execute {
        Thread.sleep((math.random() * 1000).toInt)
        val colStats = db.collection("stats")
        for {
          exists <- colStats.exists().toScala
          _ <- if (!exists) colStats.create().toScala else Future.successful(())
          _ <- if (resetToZero) colStats.truncate().toScala else Future.successful(())
          r <- colStats.insertDocument(Counter(null, nSuccess.get())).toScala
        } yield r.getNew
      })
  }

  def logStats(): Unit = synchronized {
    val ns = nStarted.get()
    val nf = nFailure.get()
    val nd = nSuccess.get()

    log.info(s"Actions started   : $ns")
    log.info(s"Actions finished  : ${nf + nd} ($nf failed, $nd succeeded)")
    log.info(s"Actions pending   : ${ns - nd - nf}")
  }
}
