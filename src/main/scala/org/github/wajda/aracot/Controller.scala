package org.github.wajda.aracot

import com.arangodb.async.ArangoDatabaseAsync
import za.co.absa.spline.common.AsyncCallRetryer
import za.co.absa.spline.persistence.ArangoImplicits.ArangoDatabaseAsyncScalaWrapper
import za.co.absa.spline.persistence.RetryableExceptionUtils
import za.co.absa.spline.persistence.model.DataSource

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Awaitable, Future}

class Controller(db: ArangoDatabaseAsync, iterations: Int, maxRetries: Int, avgDelayMs: Int, ignoreRevs: Boolean) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val cnt = AtomicPersistentCounter.retryable("tx", db, maxRetries, ignoreRevs)

  private val i = new AtomicInteger(0)

  def action(): Awaitable[_] = {
    Future.traverse(1 to iterations)(_ => {
      if (avgDelayMs > 0) {
        Thread.sleep((math.random * avgDelayMs * 2).toLong + 1)
      }

      val t0 = System.nanoTime()

      val eventualRetVal = tick1()

      eventualRetVal.foreach {
        retVal =>
          val t1 = System.nanoTime()
          val dt = t1 - t0
          val j = i.incrementAndGet()
          println(s"$j:\t$retVal\t${dt / 1000000} ms")
      }
      eventualRetVal
    })
  }

  private def tick1(): Future[_] = {
    for {
      x <- cnt.get()
      y <- cnt.next()
      z <- cnt.get()
    } yield {
      require(x < y, s"$x < $y")
      require(y <= z, s"$y <= $z")
      s"${y - x}\t${z - y}"
    }
  }

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
