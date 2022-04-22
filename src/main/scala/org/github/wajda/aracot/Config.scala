package org.github.wajda.aracot

import ch.qos.logback.classic.Level
import za.co.absa.spline.persistence.ArangoConnectionURL

case class Config(
  connectionUrl: Option[ArangoConnectionURL] = None,
  logLevel: Level = Level.INFO,
  iterations: Int = 100,
  maxRetries: Int = 5,
  avgDelayMs: Int = 50,
  ignoreRevs: Boolean = false,
  resetToZero: Boolean = false,
)
