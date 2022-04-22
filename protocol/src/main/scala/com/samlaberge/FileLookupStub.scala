package com.samlaberge

trait FileLookupStub {
  def lookupFile(fileName: String): Option[Array[Byte]]
  def lookupClass(className: String): Option[Array[Byte]]
}
