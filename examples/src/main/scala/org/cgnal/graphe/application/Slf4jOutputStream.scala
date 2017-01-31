package org.cgnal.graphe.application

import java.io.{ PrintStream, ByteArrayOutputStream, OutputStream }

import org.slf4j.Logger

sealed trait LogLevel

case object Error extends LogLevel
case object Warn extends LogLevel
case object Info extends LogLevel
case object Debug extends LogLevel
case object Trace extends LogLevel

final class Slf4jOutputStream(log: Logger, level: LogLevel) extends OutputStream {

  private var isClosed = false

  private  def whileOpen(f: => Unit) =
    if (!isClosed) f
    else throw new IllegalStateException(s"Cannot write to a closed output stream")

  private val buffer = new ByteArrayOutputStream()

  def write(bytes: Int): Unit = whileOpen { buffer.write(bytes) }

  private def printBytes() = level match {
    case Error => log.error { new String(buffer.toByteArray) }
    case Warn  => log.warn  { new String(buffer.toByteArray) }
    case Info  => log.info  { new String(buffer.toByteArray) }
    case Debug => log.debug { new String(buffer.toByteArray) }
    case Trace => log.trace { new String(buffer.toByteArray) }
  }

  override def flush() = whileOpen {
    if (buffer.size() > 0) {
      printBytes()
      buffer.reset
    }
  }

  override def close() = isClosed = true

  def asStream = new PrintStream(this, true)

}

object Slf4jOutputStream {

  def error(log: Logger) = new Slf4jOutputStream(log, Error)

  def warn(log: Logger)  = new Slf4jOutputStream(log, Warn)

  def info(log: Logger)  = new Slf4jOutputStream(log, Info)

  def debug(log: Logger) = new Slf4jOutputStream(log, Debug)

  def trace(log: Logger) = new Slf4jOutputStream(log, Trace)

}
