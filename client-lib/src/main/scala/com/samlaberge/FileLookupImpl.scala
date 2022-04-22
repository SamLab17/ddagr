package com.samlaberge

import com.google.protobuf.ByteString
import com.samlaberge.protos.fileLookup._
import io.grpc.Status

import java.io.{File, FileNotFoundException, IOException}
import java.nio.file.Files
import scala.concurrent.Future

/**
 * Class & file resolution server.
 * On receipt of class name, looks for local class files which can fulfill request
 * Does the same for local files (for inputs to jobs)
 * @param classDir Class directory for client program
 */
class FileLookupImpl(classDir: String) extends FileLookupServiceGrpc.FileLookupService with Logging {

  // Convert com.package.className to com/package/className
  private def classNameToFilePath(className: String): String =
    className.replaceAll("\\.", File.separator) + ".class"

  private def getLibClassDirectory: String =
    this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath

  private def readClassFile(className: String): Option[ByteString] = {
    // Class files could be in one of two directories: the client library
    // or the client program
    val pathInClientProg = classDir + classNameToFilePath(className)
    val pathInLib = getLibClassDirectory + classNameToFilePath(className)
    if(new File(pathInClientProg).isFile)
      readFile(pathInClientProg)
    else if(new File(pathInLib).isFile)
      readFile(pathInLib)
    else
      None
//    logDebug(s"Class full path: $fullPath")
//    readFile(fullPath)
  }

  private def readFile(fileName: String): Option[ByteString] = {
    try {
      val bytes = Files.readAllBytes(new File(fileName).toPath)
      Some(ByteString.copyFrom(bytes))
    } catch {
      case _: FileNotFoundException => None
      case _: IOException => None
      case e =>
        System.err.println(s"Unexpected exception: $e")
        None
    }
  }

  def returnFileLookupResult(bytes: Option[ByteString], requestFile: String) : Future[LookupFileResult] = {
    bytes match {
      case Some(b) => Future.successful(LookupFileResult(b))
      case None =>
        Future.failed(Status
          .NOT_FOUND
          .augmentDescription(s"Requested class, $requestFile, not found.")
          .asRuntimeException())
    }
  }

  override def lookupClass(request: LookupClassParam): Future[LookupFileResult] = {
    val classDef = readClassFile(request.className)
    classDef match {
      case Some(_) =>
//        logDebug(s"Class ${request.className} requested and found, sending.")
      case None => log(s"Class ${request.className} was requested but could not be found.")
    }
    returnFileLookupResult(classDef, request.className)
  }

  override def lookupFile(request: LookupFileParam): Future[LookupFileResult] = {
    val fileContents = readFile(request.fileName)
    fileContents match {
      case Some(_) =>
//        logDebug(s"File ${request.fileName} requested and found, sending.")
      case None => log(s"File ${request.fileName} was requested but could not be found.")
    }
    returnFileLookupResult(fileContents, request.fileName)
  }
}
