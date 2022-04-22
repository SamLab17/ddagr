package com.samlaberge

import io.grpc.{Server, ServerBuilder}
import com.samlaberge.SystemConfig.{MAX_MESSAGE_SIZE, PORTS}
import com.samlaberge.protos.scheduler.{SchedulerClientGrpc, SchedulerExecutorGrpc}

import java.net.InetAddress
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

object SchedulerMain extends App with Logging {

  log("Scheduler server starting...")
  log(s"IP          : ${InetAddress.getLocalHost.getHostAddress}")
  log(s"Hostname    : ${InetAddress.getLocalHost.getHostName}")
  log(s"Client port : ${PORTS.SCHEDULER_CLIENT_SERVER}")

  val execManager = new ExecutorManager()

  val execContext = ExecutionContext.global
  val schedulerServer: Server = ServerBuilder
    .forPort(PORTS.SCHEDULER_CLIENT_SERVER)
    .maxInboundMessageSize(MAX_MESSAGE_SIZE)
    .addServices(List(
      SchedulerClientGrpc.bindService(new ClientService(execManager), execContext),
      SchedulerExecutorGrpc.bindService(new ExecutorService(execManager), execContext)
    ).asJava)
    .build

  schedulerServer.start()

  sys.addShutdownHook {
    log("Scheduler shutting down...")
    schedulerServer.shutdown()
  }

  schedulerServer.awaitTermination()
}
