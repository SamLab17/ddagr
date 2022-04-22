package com.samlaberge

import com.samlaberge.protos.scheduler.{ExecutorInfo, ExecutorInfoParams, ExecutorInfoResult, ExecutorInitParams, ExecutorInitResult, ExecutorPartitionParams, ExecutorPartitionResult, LookupClientFileContents, LookupClientFileParams, SchedulerExecutorGrpc}

import scala.concurrent.Future

class ExecutorService(manager: ExecutorManager) extends SchedulerExecutorGrpc.SchedulerExecutor with Logging {
  override def executorInit(request: ExecutorInitParams): Future[ExecutorInitResult] = {
    // Add executor to executor manager object
    logDebug(s"Received executor init call from ${request.executorIp}:${request.port}")
    val execId = manager.addExecutor(request.executorIp, request.port)
    Future.successful(ExecutorInitResult(execId))
  }

  override def getExecutorsInfo(request: ExecutorInfoParams): Future[ExecutorInfoResult] = {
    Future.successful(
      ExecutorInfoResult(request.executorIds.map(eId => {
        val execInfo = manager.getSysInfoFor(eId)
        val (ip, port) = manager.getConnectionInfoFor(eId)
        ExecutorInfo(
          executorId = eId,
          executorIp = ip,
          executorPort = port,
          nCpus = execInfo.nCpus,
          freeMem = execInfo.freeMemory
        )
      })
      )
    )
  }

  override def requestExecutorsForPartition(request: ExecutorPartitionParams): Future[ExecutorPartitionResult] = {
    Future.successful(manager.getExecutorsForPartition(request))
  }

  override def lookupClientFile(request: LookupClientFileParams): Future[LookupClientFileContents] = {
    manager.lookupClientFile(request)
  }
}
