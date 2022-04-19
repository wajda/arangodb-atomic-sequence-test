package org.github.wajda.aracot

import com.arangodb.async.ArangoDatabaseAsync
import za.co.absa.spline.common.AsyncCallRetryer
import za.co.absa.spline.persistence.ArangoImplicits._
import za.co.absa.spline.persistence.RetryableExceptionUtils
import za.co.absa.spline.persistence.model.CollectionDef

import java.lang.reflect.{Method, Proxy}
import scala.concurrent.{ExecutionContext, Future}

trait AtomicPersistentCounter {
  def get(): Future[Long]
  def next(): Future[Long]
}

object AtomicPersistentCounter {

  def apply(name: String, db: ArangoDatabaseAsync, ignoreRevs: Boolean)(implicit ec: ExecutionContext): AtomicPersistentCounter = new AtomicPersistentCounter {
    private val aqlGet =
      s"""
         |WITH ${CollectionDef.Counter.name}
         |RETURN DOCUMENT("${CollectionDef.Counter.name}/$name").curVal
      """.stripMargin

    private val aqlNext =
      s"""
         |WITH ${CollectionDef.Counter.name}
         |LET cnt = DOCUMENT("${CollectionDef.Counter.name}/$name")
         |UPDATE cnt
         |  WITH { curVal: cnt.curVal + 1 } IN ${CollectionDef.Counter.name}
         |  OPTIONS { ignoreRevs: ${ignoreRevs} }
         |  RETURN NEW.curVal
      """.stripMargin

    def get(): Future[Long] = db.queryOne[Long](aqlGet)

    def next(): Future[Long] = db.queryOne[Long](aqlNext)
  }

  def retryable(name: String, db: ArangoDatabaseAsync, maxRetries: Int, ignoreRevs: Boolean)(implicit ec: ExecutionContext): AtomicPersistentCounter = {
    val retryer = new AsyncCallRetryer(RetryableExceptionUtils.isRetryable, maxRetries)
    val counter = AtomicPersistentCounter(name, db, ignoreRevs)
    Proxy.newProxyInstance(
      ClassLoader.getSystemClassLoader,
      Array(classOf[AtomicPersistentCounter]),
      (_: Any, method: Method, args: Array[AnyRef]) =>
        retryer.execute(method.invoke(counter, args: _*).asInstanceOf[Future[Long]])
    ).asInstanceOf[AtomicPersistentCounter]
  }
}
