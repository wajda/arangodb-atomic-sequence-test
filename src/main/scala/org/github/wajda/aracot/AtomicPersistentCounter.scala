package org.github.wajda.aracot

import com.arangodb.async.ArangoDatabaseAsync
import za.co.absa.spline.common.AsyncCallRetryer
import za.co.absa.spline.persistence.ArangoImplicits._
import za.co.absa.spline.persistence.RetryableExceptionUtils
import za.co.absa.spline.persistence.model.CollectionDef

import java.lang.reflect.{Method, Proxy}
import scala.concurrent.{ExecutionContext, Future}

trait AtomicPersistentCounter {
  def checkInitialized(): Future[Boolean]
  def get(): Future[Long]
  def next(): Future[Long]
  def reset(): Future[Unit]
}

object AtomicPersistentCounter {

  def apply(name: String, db: ArangoDatabaseAsync, ignoreRevs: Boolean)(implicit ec: ExecutionContext): AtomicPersistentCounter = new AtomicPersistentCounter {

    private val aqlIsInited =
      s"""
         |WITH ${CollectionDef.Counter.name}
         |RETURN DOCUMENT("${CollectionDef.Counter.name}/$name") != null
      """.stripMargin

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
         |  OPTIONS { ignoreRevs: $ignoreRevs }
         |  RETURN NEW.curVal
      """.stripMargin

    private val resetAql =
      s"""
         |WITH ${CollectionDef.Counter.name}
         |UPSERT { _key: "$name" }
         |INSERT { _key: "$name", curVal: 0 }
         |UPDATE { curVal: 0 } IN ${CollectionDef.Counter.name}
         |RETURN NEW.curVal
      """.stripMargin

    def checkInitialized(): Future[Boolean] = db.queryOne[Boolean](aqlIsInited)

    def get(): Future[Long] = db.queryOne[Long](aqlGet)

    def next(): Future[Long] = db.queryOne[Long](aqlNext)

    def reset(): Future[Unit] = db.queryOne[Long](resetAql).map(_ => ())
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
