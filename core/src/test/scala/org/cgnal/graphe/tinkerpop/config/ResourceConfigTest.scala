package org.cgnal.graphe.tinkerpop.config

import org.scalatest.{ MustMatchers, WordSpec }

import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider
import org.cgnal.graphe.tinkerpop.config.helper.ResourceConfigHelper

class ResourceConfigTest
  extends WordSpec
  with MustMatchers
  with ResourceConfigHelper {

  "Resource Config" when {

    "working with defaults" must {

      "load values correctly" in withConfig { conf =>
        conf.getInt("application.back-off-time")                mustBe 1500
        conf.getInt("application.retries")                      mustBe 5
        conf.getInt("application.batch-size")                   mustBe 100
        conf.getInt("cluster.max-partitions")                   mustBe 16
        conf.getBoolean("ioformat.filter-partitioned-vertices") mustBe true
      }

    }

    "working with a specific file" must {

      "load values correctly" in withConfig("custom-config.yml") { conf =>
        conf.getInt("application.back-off-time")                mustBe 2000
        conf.getInt("application.retries")                      mustBe 1
        conf.getInt("application.batch-size")                   mustBe 1000
      }

    }

  }

}
