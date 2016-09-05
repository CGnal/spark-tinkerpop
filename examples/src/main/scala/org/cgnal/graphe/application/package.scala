package org.cgnal.graphe

import java.io.{ FileNotFoundException, File }

import org.apache.commons.cli.MissingArgumentException

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{ Failure, Success, Try }

import org.rogach.scallop.Scallop

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import org.cgnal.graphe.tinkerpop.titan.TitanGraphProvider

package object application {

  implicit def tinkerGraphProvider = TitanGraphProvider

  implicit class EnrichedRDD[A](rdd: RDD[A]) {

    /**
     * Shows the first `lines` number of rows in this `RDD` and prints the values in tabular format
     * @param lines number of lines to show in the table
     */
    def show(lines: Int)(implicit A: TypeTag[A], ev: A <:< Product) = {
      val charLimit = 30
      val line     = Seq.fill(charLimit) { "-" }.mkString
      val headLine = Seq.fill(charLimit) { "=" }.mkString

      val headers = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType].fieldNames.toSeq.map { name => f"${name.shorten(charLimit)}%-30s" }
      val header  = headers.mkString("|", "|", "|")
      val (countLines, bodyLines) = rdd.toLocalIterator.foldLeft(0l -> Seq.empty[A]) { (acc, next) =>
        acc match {
          case (count, accLines) if count <= lines => (count + 1, accLines :+ next)
          case (count, accLines)                   => (count + 1, accLines)
        }
      }
      val body    = bodyLines.map {
        ev(_).productIterator.map {
          case null            => "null"
          case array: Array[_] => s"[${array.mkString(", ").shorten(charLimit)}]"
          case seq: Seq[_]     => s"[${seq.mkString(", ").shorten(charLimit)}]"
          case other           => other.toString.shorten(charLimit)
        }.map { value => f"$value%-30s" }.mkString("|", "|", "|")
      }
      val fullLineTop    = Seq.fill(headers.size) { line }.mkString("+", "-", "+")
      val fullLineBottom = Seq.fill(headers.size) { line }.mkString("+", "-", "+")
      val fullHeadLine   = Seq.fill(headers.size) { headLine }.mkString("|", "=", "|")
      val statsLine = s"Total Rows: $countLines"

      { fullLineTop +: header +: fullHeadLine +: body :+ fullLineBottom :+ statsLine :+ fullLineBottom }.foreach { println }
    }

    def shortenedName(charLimit: Int): String = Option { rdd.name }.map { _ shorten charLimit } getOrElse s"[RDD${rdd.id}]"

    def shortName = shortenedName(50)

    def persist(name: String, immediate: Boolean = true): RDD[A] = {
      rdd.setName(name)
      rdd.persist()
      if (immediate) rdd.foreach { _ => () } // do nothing
      rdd
    }

  }

  implicit class EnrichedString(s: String) {

    def shorten(charLimit: Int = 50) = s.take(charLimit) + { if (s.length > charLimit) "..." else "" }

    private def asFile(path: String) = Try { new File(path) }.flatMap { file =>
      if (file.exists()) Success { file }
      else Failure { new FileNotFoundException(s"Unable to find file [$path]") }
    }

    private def asList(path: String): Try[List[String]] = for {
      file <- asFile(path)
    } yield {
      if (file.getName endsWith ".jar") List { file.getAbsolutePath }
      else if (file.isDirectory) file.listFiles().toList.collect {
        case file if file.getName endsWith ".jar" => file.getAbsolutePath
      }
      else List.empty
    }

    def asPathList: Try[List[String]] = Try { s split "," map { _.trim } }.flatMap {
      _.toList.foldLeft(Try(List.empty[String])) { (acc, next) =>
        for {
          all <- acc
          n   <- asList(next)
        } yield n ::: all
      }
    }

  }

  implicit class EnrichedScallop(scallop: Scallop) {

    def mandatory[A](name: String)(implicit A: ClassTag[A], TA: TypeTag[A]) = scallop.get[A](name) match {
      case Some(a) => a
      case None    => throw new MissingArgumentException(s"Unable to find mandatory argument [$name] of type [${A.runtimeClass.getCanonicalName}]")
    }

  }

}