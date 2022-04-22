package com.samlaberge

import com.google.protobuf.empty.Empty
import com.samlaberge.SystemConfig.MAX_MESSAGE_SIZE
import com.samlaberge.Util._
import com.samlaberge.protos.executor.ExecutorGrpc.ExecutorStub
import com.samlaberge.protos.executor._
import com.samlaberge.protos.scheduler.SchedulerExecutorGrpc.SchedulerExecutorBlockingStub
import com.samlaberge.protos.scheduler.{ExecutorInfoParams, ExecutorPartitionParams, LookupClientFileParams}
import io.grpc.{ManagedChannelBuilder, Status, StatusRuntimeException}
import org.apache.commons.io.IOUtils

import java.io.ByteArrayInputStream
import java.net.{InetAddress, URL}
import java.util.concurrent
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPInputStream
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

// This server is highly multi-threaded, will need to be thread-safe.

class ExecutorServerImpl(schedulerStub: SchedulerExecutorBlockingStub) extends ExecutorGrpc.Executor with Logging {
  override def getSysInfo(request: Empty): Future[ExecutorSysInfo] = {
    Future.successful(ExecutorSysInfo(
      hostname = InetAddress.getLocalHost.getHostName,
      nCpus = Runtime.getRuntime.availableProcessors(),
      maxMemSize = Runtime.getRuntime.maxMemory(),
      freeMemory = Runtime.getRuntime.freeMemory()
    ))

  }

  case class ClientInfo(
    id: Int,
    //    fileService: FileLookupServiceBlockingClient,
    fileStub: FileLookupStub,
    classLoader: ClassLoader,
    intermediateOutputs: ConcurrentHashMap[Int, Any],
    currentThreads: mutable.ListBuffer[Thread],
  )

  val clientInfos = new concurrent.ConcurrentHashMap[Int, ClientInfo]()

  val executorStubs = new ConcurrentHashMap[Int, ExecutorStub]()

  def getStubFor(execId: Int): ExecutorStub = {
    executorStubs.synchronized {
      if (executorStubs.containsKey(execId))
        executorStubs.get(execId)
      else {
        val executorInfo = schedulerStub.getExecutorsInfo(ExecutorInfoParams(Seq(execId))).executors.head
        val channel = ManagedChannelBuilder.forAddress(executorInfo.executorIp, executorInfo.executorPort).maxInboundMessageSize(MAX_MESSAGE_SIZE).usePlaintext().build
        val stub = ExecutorGrpc.stub(channel)
        executorStubs.putIfAbsent(execId, stub)
        stub
      }
    }
  }


  override def newClientSession(request: NewClientSessionParams): Future[Empty] = {
    // Connect to client's file service for file/class resolution
    //    val clientChannel = ManagedChannelBuilder.forAddress(request.clientIp, request.clientPort).usePlaintext().build
    //    val fileStub = FileLookupServiceGrpc.blockingStub(clientChannel)
    val fileStub = new FileLookupStub {
      override def lookupFile(fileName: String): Option[Array[Byte]] = {
        try {
          Some(schedulerStub.lookupClientFile(
            LookupClientFileParams(request.clientId, isClass = false, fileName)).fileContents
          )
        } catch {
          case _: Throwable => None
        }
      }

      override def lookupClass(className: String): Option[Array[Byte]] = {
        try {
          Some(schedulerStub.lookupClientFile(
            LookupClientFileParams(request.clientId, isClass = true, className)).fileContents
          )
        } catch {
          case _: Throwable => None
        }
      }
    }

    val parentCL = Thread.currentThread().getContextClassLoader
    val classLoader = new NetworkClassLoader(parentCL, fileStub)

    clientInfos.putIfAbsent(request.clientId, ClientInfo(
      request.clientId,
      fileStub,
      classLoader,
      new ConcurrentHashMap(),
      new ListBuffer[Thread](),
    ))
    Future.successful(Empty())
  }

  override def endClientSession(request: EndClientSessionParams): Future[Empty] = {
    val clientInfo = clientInfos.get(request.clientId)
    clientInfo.synchronized {
      clientInfo.currentThreads.foreach(t => t.interrupt())
      clientInfos.remove(request.clientId)
      clientInfo.intermediateOutputs.clear()
    }
    Future.successful(Empty())
  }

  override def runTask(request: TaskParam): Future[TaskResult] = {
    val clientInfo = clientInfos.get(request.clientId)
    val result = Try {
      // add self to currentThreads for job
      clientInfo.currentThreads.synchronized {
        clientInfo.currentThreads.addOne(Thread.currentThread())
      }
      // run task

      // advise gc to run before we run the task (potentially clean up
      // memory left over from previous task)
      //      System.gc()

      val operation = deserialize[ExecutorInstruction](request.operation, clientInfo.classLoader)
      logDebug(s"Operation to run: $operation")
      executeOperation(operation, clientInfo)
    }

    // FIXME: could this thread get interrupted right here? is that a problem?

    // remove self from currentThreads
    clientInfo.currentThreads.synchronized {
      clientInfo.currentThreads.filterInPlace(_.getId != Thread.currentThread().getId)
    }


    result match {
      case Success(output) => {
        Future.successful(TaskResult(serialize(output)))
      }
      case Failure(e: InterruptedException) => {
        log("Thread running task was interrupted. Job ended")
        //        Future.successful(TaskResult(status=TERMINATED, output=Array[Byte](0)))
        Future.failed(e)
      }
      case Failure(e: Throwable) => {
        logErr(s"Client provided code threw an exception.", e)
        Future.failed(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Client-provided lambda threw an exception.")))
      }
    }
  }


  def executeOperation(instr: ExecutorInstruction, clientInfo: ClientInfo): OperationResult = {
    // Execute a top-level operation (one that requires a response sent back to the scheduler)
    instr match {
      case CountInstruction(input) => {
        CountOpResult(executeInstructions(input, clientInfo).asInstanceOf[Seq[_]].size)
      }
      case CollectInstruction(input) => {
        CollectOpResult(executeInstructions(input, clientInfo).asInstanceOf[Seq[_]])
      }
      case CountGroupedInstruction(input) => {
        CountGroupedOpResult(executeInstructions(input, clientInfo).asInstanceOf[Map[_, _]].size)
      }
      case CollectGroupedInstruction(input) => {
        CollectGroupedOpResult(executeInstructions(input, clientInfo).asInstanceOf[Map[Any, Any]])
      }
      case RepartitionInstruction(input, nPartitions, outputId) =>
        handleRepartition(
          executeInstructions(input, clientInfo).asInstanceOf[Seq[_]],
          nPartitions,
          outputId,
          clientInfo
        )
      case ReduceAndSendInstruction(input, reduceFn, execId, outputId) => {
        handleReduceAndSend(
          executeInstructions(input, clientInfo).asInstanceOf[Seq[_]],
          reduceFn,
          execId,
          outputId,
          clientInfo
        )
      }
      case GroupByInstruction(input, keyFn, execs, outputId) => {
        handleGroupBy(
          executeInstructions(input, clientInfo).asInstanceOf[Seq[_]],
          keyFn,
          execs,
          outputId,
          clientInfo
        )
      }
      case _ => {
        throw new IllegalArgumentException(s"Invalid top-level instruction given: ${instr.getClass.getName}")
      }
    }
  }

  def executeInstructions(instr: ExecutorInstruction, clientInfo: ClientInfo): Any = {
    instr match {
      case StageInputInstruction(stageId) => {
        if (clientInfo.intermediateOutputs containsKey stageId) {
          clientInfo.intermediateOutputs.get(stageId)
        } else {
          throw new IllegalArgumentException(s"Instruction depends on output of stage ${stageId}, but not present.")
        }
      }

      case FromSeqInputInstruction(data) => data

      case ClientTextFileInputInstruction(fileName) => {
        handleClientFile(fileName, clientInfo.fileStub)
      }

      case UrlTextFileInputInstruction(fileUrl) => {
        handleUrlFile(fileUrl)
      }

      case ClientMultipleTextFileInputInstruction(fileNames) => {
        fileNames.map(f => handleClientFile(f, clientInfo.fileStub)).reduce(_ ++ _)
      }

      case UrlMultipleTextFileInputInstruction(urls) => {
        urls.par.map(u => handleUrlFile(u)).seq.reduce(_ ++ _)
      }

      case MapInstruction(input, mapFn) => {
        // FIXME have cutoff for .par?
        executeInstructions(input, clientInfo).asInstanceOf[Seq[_]].par.map(mapFn).seq
      }

      case FlatMapInstruction(input, flatMapFn) => {
        executeInstructions(input, clientInfo).asInstanceOf[Seq[_]].par.flatMap(flatMapFn).seq
      }

      case FilterInstruction(input, filterFn) => {
        val filterInput = executeInstructions(input, clientInfo).asInstanceOf[Seq[_]]
        filterInput.filter(filterFn).seq
      }

      case FinishReduceInstruction(input, reduceFn) => {
        Seq(executeInstructions(input, clientInfo).asInstanceOf[Seq[_]].par.reduce(reduceFn))
      }

      case FinishGroupBy(input, keyFn) => {
        val data = executeInstructions(input, clientInfo).asInstanceOf[Seq[_]]
        logDebug(s"Finishing groupBy for ${data.size} entries")
        // TODO: Is .par worth all of this .view.mapValues().toMap cruft?
        data.par.groupBy(keyFn).seq.view.mapValues(_.seq).toMap
      }

      case MapValuesInstruction(input, mapFn) => {
        val data = executeInstructions(input, clientInfo).asInstanceOf[Map[_, _]]
        data.view.mapValues(_.asInstanceOf[Seq[_]].map(mapFn)).toMap
      }

      case FromKeysInstruction(input) => {
        executeInstructions(input, clientInfo).asInstanceOf[Map[_, _]].keys.toSeq
      }

      case ReduceGroupsInstruction(input, reduceFn) => {
        executeInstructions(input, clientInfo).asInstanceOf[Map[_, _]].view.mapValues(_.asInstanceOf[Seq[_]].reduce(reduceFn)).toSeq
      }

      case _ => {
        throw new IllegalArgumentException(s"Invalid intermediate instruction encountered: $instr")
      }

    }
  }

  val MIN_PARTITION_SIZE = 1

  def handleRepartition(input: Seq[_], n: Int, outputId: Int, clientInfo: ClientInfo): RepartitionOpResult = {
    // Ask for executors
    val nPartitions = n min (input.size / MIN_PARTITION_SIZE)
    val executors = schedulerStub.requestExecutorsForPartition(ExecutorPartitionParams(clientInfo.id, nPartitions)).executorIds
    val executorInfos = schedulerStub.getExecutorsInfo(ExecutorInfoParams(executors)).executors

    assert(executors.size == executorInfos.size && executors.distinct.size == executors.size)

    logDebug(s"Partitioning ${input.size} elements across ${executors.size} executors")

    // TODO: Make this smarter to take executor memory sizes/nCpus into account
    // Divides and rounds up
    val sizePerExecutor = (input.size + executors.size - 1) / executors.size


    var i = 0
    val futures = executors.map(eid => {
      val executorStub = getStubFor(eid)

      logDebug(s"Sending intermediate data to executor ${eid}"
        + s"(size<=${sizePerExecutor}, clientId= ${clientInfo.id}, intermediateId=$outputId)")

      val data = input.slice(i * sizePerExecutor, (i + 1) * sizePerExecutor min input.size)
      i = i + 1

      executorStub.sendIntermediateData(
        SendIntermediateParam(
          clientInfo.id,
          outputId,
          serialize(data)
        ))
    })

    // Now wait on all of the transfers to complete.
    logDebug("Waiting for all partition transfers to complete")
    Await.result(Future.sequence(futures), Duration.Inf)
    logDebug("Repartition complete.")

    RepartitionOpResult(executors)
  }

  def handleReduceAndSend(input: Seq[_], reduceFn: (Any, Any) => Any, execId: Int, outputId: Int, clientInfo: ClientInfo): ReduceAndSendResult = {
    val output = Seq(input.par.reduce(reduceFn))
    val stub = getStubFor(execId)

    val future = stub.sendIntermediateData(SendIntermediateParam(clientInfo.id, outputId, serialize(output)))
    Await.result(future, Duration.Inf)

    ReduceAndSendResult(execId)
  }

  def handleGroupBy(input: Seq[_], keyFn: Any => Any, execs: Seq[ExecutorRingPosition], outputId: Int, clientInfo: ClientInfo): GroupByOpResult = {

    val sortedExecs = execs.sortBy(_.ringPosition)
    //    logDebug(s"Sorted Execs: $sortedExecs")

    def getExecIdForEntry(entry: Any): Int = {
      val hash = keyFn(entry).hashCode()
      // Find first node whose position is larger than the hash code
      val exec = sortedExecs.find(_.ringPosition > hash)
      // (or the first node in the list)
      exec.getOrElse(sortedExecs.head).executorId
    }

    val groupedByDestination = input.par.groupBy(getExecIdForEntry).seq

    val futures = groupedByDestination.map {
      case (eid, data) => {
        val stub = getStubFor(eid)
        logDebug(s"Sending ${data.size} entries to $eid")
        stub.sendIntermediateData(SendIntermediateParam(clientInfo.id, outputId, serialize(data.seq)))
      }
    }

    Await.result(Future.sequence(futures), Duration.Inf)
    logDebug(s"All entries sent")
    GroupByOpResult(groupedByDestination.keys.toSeq)
  }

  def handleClientFile(fileName: String, fileService: FileLookupStub): Seq[String] = {
    logDebug(s"Getting client file $fileName ...")
    val start = System.currentTimeMillis()
    val res = fileService.lookupFile(fileName).get
    val nSeconds = (System.currentTimeMillis() - start) / 1000
    log(s"Received $fileName in $nSeconds seconds (${"%.2f" format (res.length / 1024.0 / 1024.0 / nSeconds)} MB/s)")
    val lines = decompressIfNeeded(fileName, res)
    lines
  }

  def handleUrlFile(fileUrl: String): Seq[String] = {
    logDebug(s"Getting URL file $fileUrl ...")
    decompressIfNeeded(fileUrl, downloadFile(fileUrl))
  }

  def downloadFile(url: String): Array[Byte] = {
    val start = System.currentTimeMillis()
    val res = IOUtils.toByteArray(new URL(url))
    val nSeconds = (System.currentTimeMillis() - start) / 1000
    log(s"Downloaded $url in $nSeconds seconds (${"%.2f" format (res.length / 1024.0 / 1024.0 / nSeconds)} MB/s)")
    res
  }

  def decompressIfNeeded(fileNameOrUrl: String, fileContents: Array[Byte]): Seq[String] = {
    // leave unchanged if no decompression necessary
    if (fileContents(0) == GZIPInputStream.GZIP_MAGIC.toByte
      && fileContents(1) == (GZIPInputStream.GZIP_MAGIC >> 8).toByte) {
      logDebug(s"$fileNameOrUrl is gzip compressed. Decompressing")
      // File is gzip compressed
      Source.fromInputStream(new GZIPInputStream(new ByteArrayInputStream(fileContents))).getLines().toSeq
    } else {
      // File not compressed
      Source.fromBytes(fileContents).getLines().toSeq
    }
  }

  // Receiving partition from another executor
  override def sendIntermediateData(request: SendIntermediateParam): Future[Empty] = {
    val clientInfo = clientInfos.get(request.clientId)
    // TODO this assumes partitions are a Seq
    var partitionData = deserialize[Seq[_]](request.data, clientInfo.classLoader)
    val persist = clientInfo.intermediateOutputs

    logDebug(s"Received intermediate data (clientId=${
      request.clientId
    }, intermediateId=${
      request.intermediateId
    })")

    persist.synchronized {
      if (persist.containsKey(request.intermediateId)) {
        // Concatenate the two
        partitionData = persist.get(request.intermediateId).asInstanceOf[Seq[_]] ++ partitionData
      }
      persist.put(request.intermediateId, partitionData)
      Future.successful(Empty())
    }

  }

}
