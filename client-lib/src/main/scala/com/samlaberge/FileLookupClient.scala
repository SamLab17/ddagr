package com.samlaberge

import java.io._
import java.net.Socket
import java.nio.file.Files

class FileLookupClient(serverHost: String, serverPort: Int, classDir: String) extends Logging {

  private val thread: Thread = new Thread(() => {
    val socket = new Socket(serverHost, serverPort)
    logDebug(s"Connected to file lookup server!")

    val in = socket.getInputStream
    val ois = new ObjectInputStream(in)
    try {
      while(true) {
        val request = ois.readObject().asInstanceOf[FileLookupRequest]

        val result = request match {
          case FileLookup(fileName) => lookupFile(fileName)
          case ClassFileLookup(className) => lookupClass(className)
        }

        val oos = new ObjectOutputStream(socket.getOutputStream)
        oos.writeObject(result)
      }
    } catch {
      case e: Throwable =>
    } finally {
      in.close()
      socket.close()
    }
  })

  thread.start()

  def stopServer() : Unit = {
    thread.interrupt()
  }

  private def classNameToFilePath(className: String): String =
    className.replaceAll("\\.", File.separator) + ".class"

  private def getLibClassDirectory: String =
    this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath

  private def readClassFile(className: String): Option[Array[Byte]] = {
    // Class files could be in one of two directories: the client library
    // or the client program
    val pathInClientProg = classDir + classNameToFilePath(className)
    val pathInLib = getLibClassDirectory + classNameToFilePath(className)
    if(new File(pathInClientProg).isFile) {
      readFile(pathInClientProg)
    } else if(new File(pathInLib).isFile) {
      readFile(pathInLib)
    } else {
      None
    }
  }

  private def readFile(fileName: String): Option[Array[Byte]] = {
    try {
      Some(Files.readAllBytes(new File(fileName).toPath))
    } catch {
      case _: FileNotFoundException => None
      case _: IOException => None
      case e: Throwable =>
        System.err.println(s"Unexpected exception: $e")
        None
    }
  }

  private def lookupFile(fileName: String): FileLookupResult = {
    FileLookupResult(readFile(fileName))
  }

  private def lookupClass(className: String): FileLookupResult = {
     FileLookupResult(readClassFile(className))
  }




}
