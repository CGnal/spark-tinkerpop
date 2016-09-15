package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

sealed trait SecurityConfig extends Config

case class KerberosConfig(user: String, keytabLocation: String) extends SecurityConfig

case object NoSecurityConfig extends SecurityConfig

object SecurityConfig {

  def fromString(arg: String): SecurityConfig = arg.split(":") match {
    case Array(user, keytabLocation) => KerberosConfig(user, keytabLocation)
    case _                           => throw new IllegalArgumentException(s"Invalid security setting [$arg] -- must be [user]@[keytab-location]")
  }

}

object SecurityConfigReader extends ScallopConfigReader[SecurityConfig] {

  def scallopts(scallop: Scallop): Scallop = scallop
    .opt[String](name = "kerberos", short = 'k', descr = "(optional) kerberos setting [user]@[keytab-location]")

  def consumeScallop(scallop: Scallop): SecurityConfig = scallop.get[String]("kerberos").map { SecurityConfig.fromString }.getOrElse { NoSecurityConfig }

}
