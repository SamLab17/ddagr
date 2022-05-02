package com.samlaberge

sealed trait FileLookupRequest {}
case class FileLookup(fileName: String) extends FileLookupRequest
case class ClassFileLookup(className: String) extends FileLookupRequest

case class FileLookupResult(contents: Option[Array[Byte]])
