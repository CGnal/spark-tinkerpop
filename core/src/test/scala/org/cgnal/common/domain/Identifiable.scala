package org.cgnal.common.domain

trait Identifiable {

  def id: Long

  def relatesTo[B <: Identifiable](other: B): Connection

}
