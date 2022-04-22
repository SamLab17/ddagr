package com.samlaberge

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.ServerSocket
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable

class FileLookupServer(port: Int) extends FileLookupStub with Logging {
  private val requests = new LinkedBlockingQueue[FileLookupRequest]()
  private val results = new LinkedBlockingQueue[FileLookupResult]()

  // The server will cache class definitions to prevent messages
  // to the client for classes that have already been looked-up
  // (This happens a lot when many executors all need to access
  // the same client class when running client code)
  val classCache = new mutable.HashMap[String, Array[Byte]]()

  private val thread = new Thread (() =>{
    logDebug(s"Starting File Lookup server on port $port")
    val ss = new ServerSocket(port)
    val socket = ss.accept()
    val oos = new ObjectOutputStream(socket.getOutputStream)
    try {
      while(true) {
        val req = requests.take()
        oos.writeObject(req)
        val ois = new ObjectInputStream(socket.getInputStream)
        results.put(ois.readObject().asInstanceOf[FileLookupResult])
      }
    } catch {
      case e: Throwable =>
    } finally {
      socket.close()
    }
  })

  thread.start()

  def stopServer(): Unit = {
    thread.interrupt()
  }

  def lookupFile(fileName: String): Option[Array[Byte]] = {
    requests.put(FileLookup(fileName))
    results.take().contents
  }

  def lookupClass(className: String): Option[Array[Byte]] = {
    if(classCache.contains(className)) {
      classCache.get(className)
    } else {
      requests.put(ClassFileLookup(className))
      val classDef = results.take().contents
      if(classDef.nonEmpty)
        classCache.put(className, classDef.get)
      classDef
    }
  }

  def getPort: Int = port
}
