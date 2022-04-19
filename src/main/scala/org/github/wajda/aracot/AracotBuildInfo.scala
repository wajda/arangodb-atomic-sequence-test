package org.github.wajda.aracot

import za.co.absa.commons.buildinfo.{BuildInfo, PropMapping}

object AracotBuildInfo extends BuildInfo("/aracot-build", PropMapping.Default) {
  lazy val Revision: String = BuildProps.getProperty("build.revision")
}
