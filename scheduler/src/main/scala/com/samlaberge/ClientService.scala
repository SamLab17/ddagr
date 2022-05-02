package com.samlaberge

import com.samlaberge.protos.scheduler._

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.Future
import Util._
import com.google.protobuf.empty.Empty
import com.samlaberge.SystemConfig.PORTS

/**
 * Service used by client to communicate with scheduler.
 * Operations and DAGs come through here.
 */
class ClientService(executorManager: ExecutorManager) extends SchedulerClientGrpc.SchedulerClient with Logging {

  case class ClientInfo(
    fileService: FileLookupServer,
    clientClassLoader: ClassLoader,
  )

  val nextClientId = new AtomicInteger();
  val clients = new mutable.HashMap[Int, ClientInfo]

  val fileServerPorts = new mutable.Queue[Int]()
  fileServerPorts.addAll(PORTS.FILE_LOOKUP_BASE to PORTS.FILE_LOOKUP_BASE + 100)

  override def clientInit(request: DdagrClientInitParams): Future[DdagrClientInitResult] = {
    val clientId = nextClientId.incrementAndGet()
//    val fileServerPort = PORTS.FILE_LOOKUP_BASE + clientId
    val fileServerPort = fileServerPorts.dequeue()
    val resp = DdagrClientInitResult(
      clientId,
      fileServerPort
    )

    logDebug(s"New client connected.")

    val fileServer = new FileLookupServer(fileServerPort)

    // Set up class loader for client
    val parentCL = Thread.currentThread().getContextClassLoader
    val classLoader = new NetworkClassLoader(parentCL, fileServer)
    clients += (clientId ->
      ClientInfo(
        fileServer,
        classLoader)
      )

    executorManager.newClient(clientId, fileServer)

    Future.successful(resp)
  }

  override def executeOperation(request: ExecuteOperationParams): Future[ExecuteOperationResult] = {
    try {
      val clientInfo = clients(request.myId)
      val op = deserialize[OperationDescriptor](request.operation, clientInfo.clientClassLoader)
      val reply = executorManager.executeOperation(op, request.myId, clientInfo.clientClassLoader)
      Future.successful(reply)
    } catch {
      case e: Throwable => logErr("Execute operation resulted in an exception", e); Future.failed(e)
    }
  }

  override def clientDisconnect(request: ClientDisconnectParams): Future[Empty] = {
    logDebug("Client disconnected.")
    executorManager.clientDisconnect(request.clientId)
    val fileServer = clients(request.clientId).fileService
    fileServer.stopServer()
    fileServerPorts.enqueue(fileServer.getPort)
    Future.successful(Empty())
  }
}
