package com.samlaberge

import com.samlaberge.SystemConfig.{MAX_MESSAGE_SIZE, PORTS}
import com.samlaberge.protos.executor.ExecutorGrpc
import com.samlaberge.protos.scheduler.{ExecutorInitParams, SchedulerExecutorGrpc}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder, StatusRuntimeException}

import java.util.concurrent
import java.net.InetAddress
import scala.concurrent.ExecutionContext

object ExecutorMain extends App with Logging {

  // TODO port should be an environment variable, so multiple executors can be run on the same machine (for testing)

  val port = sys.env.getOrElse("PORT", PORTS.EXECUTOR_SERVER_DEFAULT).toString.toInt
  val schedulerHost = sys.env.getOrElse("SCHEDULER", "localhost")

  log("Executor server starting... ")
  log(s"IP             : ${InetAddress.getLocalHost.getHostAddress}")
  log(s"Hostname       : ${InetAddress.getLocalHost.getHostName}")
  log(s"Port           : $port")
  log(s"n cpus         : ${Runtime.getRuntime.availableProcessors()}")
  log(s"max mem size   : ${Runtime.getRuntime.maxMemory() / 1024 / 1024}MB")
  log(s"scheduler host : $schedulerHost")

  // Stub to scheduler server
  val channel: ManagedChannel = ManagedChannelBuilder.forAddress(schedulerHost, PORTS.SCHEDULER_EXECUTOR_SERVER).usePlaintext().build
  val schedulerStub = SchedulerExecutorGrpc.blockingStub(channel)

  // Start executor server
  val server : Server = ServerBuilder.forPort(port).maxInboundMessageSize(MAX_MESSAGE_SIZE)
//    .executor(concurrent.Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors()))
    .addService(ExecutorGrpc.bindService(new ExecutorServerImpl(schedulerStub), ExecutionContext.global))
    .build

  server.start()


  // Connect to scheduler
  try {
    schedulerStub.executorInit(ExecutorInitParams(
      executorIp = InetAddress.getLocalHost.getHostAddress,
      port = port,
    ))
  } catch {
    case e: StatusRuntimeException => {
      logErr(s"Could not connect to the scheduler (ip=${schedulerHost}, port=${PORTS.SCHEDULER_EXECUTOR_SERVER})", e)
      System.exit(1);
    }
  }

  server.awaitTermination()
}
