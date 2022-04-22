package com.samlaberge

import com.google.protobuf.ByteString

import java.io.{ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}
import scala.language.implicitConversions

object Util {

  implicit def byteArrToByteString(a: Array[Byte]): ByteString =
    ByteString.copyFrom(a)

  implicit def byteStringToByteArray(bs: ByteString): Array[Byte] =
    bs.toByteArray

  def serialize[T](o: T): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bytes)
    oos.writeObject(o)
    bytes.toByteArray
  }

  def deserialize[T](b: Array[Byte], cl: ClassLoader = null): T = {
    val ois = if(cl != null) {
      new ObjectInputStream(b.newInput()) {
        override def resolveClass(desc: ObjectStreamClass): Class[_] = {
          Class.forName(desc.getName, false, cl)
        }
      }
    } else {
      new ObjectInputStream(b.newInput())
    }
    ois.readObject().asInstanceOf[T]
  }

}
