package org.cgnal.graphe.application

import org.apache.hadoop.security.UserGroupInformation

import scala.util.{Success, Try}

import org.apache.spark.{ SparkConf, SparkContext }

import org.cgnal.graphe.application.config.{NoSecurityConfig, KerberosConfig, ApplicationConfig}

trait SparkContextInstance {

  protected def sparkContext: SparkContext

}

trait SparkContextProvider {

  private def getClassJar = Try {
    this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
  }

  private def makeStandaloneSparkConf(config: ApplicationConfig) = Try {
    new SparkConf()
      .setMaster(s"local[${config.sparkConfig.numThreads}]")
      .set("spark.executor.memory",               config.sparkConfig.executorMemory)
      .set("spark.executor.instances",            config.sparkConfig.numThreads.toString)
      .set("spark.default.parallelism",           config.sparkConfig.numPartitions.toString)
      .set("spark.executor.extraClassPath",       config.hadoopDir)
      .set("spark.executor.extraJavaOptions",     "-Djava.net.preferIPv4Stack=true")
      .set("spark.driver.extraJavaOptions",       "-Djava.net.preferIPv4Stack=true")
  }

  private def makeSparkSubmitSparkConf(config: ApplicationConfig) = for {
    classJar <- getClassJar
    jarList  <- config.libDir.asPathList
  } yield new SparkConf().setJars { classJar :: jarList }

  private def makeSparkConf(config: ApplicationConfig): Try[SparkConf] = {
    secure(config)
    if (config.isStandalone) makeStandaloneSparkConf(config)
    else makeSparkSubmitSparkConf(config)
  }

  /**
   * Secures the application using security information defined in `config`.
   */
  private def secure(config: ApplicationConfig): Try[Unit] = config.securityConfig match {
    case KerberosConfig(user, keytabLocation) => Try { UserGroupInformation.loginUserFromKeytab(user, keytabLocation) }
    case NoSecurityConfig                     => Success { }
  }

  final protected def sparkConf(name: String, config: ApplicationConfig) = makeSparkConf(config).map { _.setAppName(name) }

  final protected def withSparkContext[A](name: String, config: ApplicationConfig)(f: SparkContext => A) = sparkConf(name, config).map { sparkConfig =>
    f(new SparkContext(sparkConfig))
  }

}
