package com.bluehonour.kafka.serializer

import java.util.Date

import scala.beans.BeanProperty


class User extends Serializable {
  @BeanProperty var id: Integer = _
  @BeanProperty var name: String = _
  @BeanProperty var birthday: Date = _

  def this(id: Int, name: String, birthday: Date){
    this()
    this.id = id
    this.name = name
    this.birthday = birthday
  }

  override def toString: String = {
    return s"User{id=${id}, name=${name}, birthday=${birthday}"
  }
}
