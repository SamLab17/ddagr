package com.samlaberge

import com.google.protobuf.empty.Empty
import com.samlaberge.SystemConfig.MAX_MESSAGE_SIZE
import com.samlaberge.Util._
import com.samlaberge.protos.executor.ExecutorGrpc.ExecutorStub
import com.samlaberge.protos.executor._
import com.samlaberge.protos.scheduler._
import io.grpc.{Context, ManagedChannelBuilder}

import java.io.FileNotFoundException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.math.abs
import scala.util.{Failure, Success}

class ExecutorManager extends Logging {


  case class Executor(
    executorId: Int,
    ip: String,
    port: Int,
    stub: ExecutorStub,
    var status: ExecutorStatus,
    var lastSysInfo: Option[ExecutorSysInfo]
  )

  sealed trait ExecutorStatus {}

  case class Unreachable() extends ExecutorStatus

  case class Idle() extends ExecutorStatus

  case class Pending(/* pending task */) extends ExecutorStatus

  case class Running(/* running task */) extends ExecutorStatus

  private val executors = new mutable.HashMap[Int, Executor]()
  private val nextExecutorId = new AtomicInteger()

  val STAGE_TIMEOUT: FiniteDuration = 1 hour

  def addExecutor(ip: String, port: Int): Int = {

    /*_*/
    val channel = ManagedChannelBuilder
      .forAddress(ip, port)
      .maxInboundMessageSize(MAX_MESSAGE_SIZE)
      .usePlaintext()
      .build
    /*_*/
    val executorStub = ExecutorGrpc.stub(channel)
    val id = nextExecutorId.getAndIncrement()

    executors += id -> Executor(
      executorId = id,
      ip = ip,
      port = port,
      executorStub,
      status = Unreachable(),
      lastSysInfo = None
    )


    val ctx = Context.current().fork()
    ctx.run(() => {
      val sysInfo = executorStub.getSysInfo(Empty())
      sysInfo onComplete {
        case Success(sysInfo) =>
          executors(id).lastSysInfo = Some(sysInfo)
          executors(id).status = Idle()
        case Failure(e) =>
          logErr(s"Failed to get system information from executor at $ip", e)
      }
    })


    id
  }

  def numExecutors: Int = executors.size

  def totalMemory: Long =
    executors.values
      .filter(_.status != Unreachable())
      .filter(_.lastSysInfo.nonEmpty)
      .map(_.lastSysInfo.get.maxMemSize)
      .sum

  def totalCpus: Int = {
    val distinctIp = executors.values.groupBy(_.ip).map(_._2.head)
    distinctIp
      .filter(_.status != Unreachable())
      .filter(_.lastSysInfo.nonEmpty)
      .map(_.lastSysInfo.get.nCpus)
      .sum
  }

  def getSysInfoFor(eId: Int): ExecutorSysInfo = {
    executors(eId).lastSysInfo.get
  }

  def getConnectionInfoFor(eId: Int): (String, Int) = {
    (executors(eId).ip, executors(eId).port)
  }

  case class ClientState(
    nextStageId: AtomicInteger,
    fileStub: FileLookupStub,
  )

  val clientState = new ConcurrentHashMap[Int, ClientState]()

  def newClient(clientId: Int, fileStub: FileLookupStub): Unit = {

    clientState.putIfAbsent(clientId, ClientState(
      new AtomicInteger(),
      fileStub
    ))

    val futures = executors.values.map(e => {
      e.stub.newClientSession(NewClientSessionParams(clientId))
    })
    // TODO: don't block here, but mark the executors as available when they respond
    Await.result(Future.sequence(futures), Duration.Inf)
    logDebug(s"All executors acknowledged new client")
  }

  def clientDisconnect(clientId: Int): Unit = {
    executors.values.foreach(e => {
      e.stub.endClientSession(EndClientSessionParams(clientId))
    })
    clientState.remove(clientId)
  }

  def lookupClientFile(request: LookupClientFileParams): Future[LookupClientFileContents] = {
    val fileStub = clientState.get(request.clientId).fileStub
    val bytes = if (request.isClass) {
      fileStub.lookupClass(request.fileName)
    } else {
      fileStub.lookupFile(request.fileName)
    }

    if (bytes.nonEmpty)
      Future.successful(LookupClientFileContents(bytes.get))
    else
      Future.failed(new FileNotFoundException(s"Could not find ${request.fileName}"))
  }

  case class Stage(
    stageId: Int,
    instructions: ExecutorInstruction,
    var result: Option[OperationResult] = None,
    var startTime: Option[Long] = None,
    var endTime: Option[Long] = None)

  def executeOperation(op: OperationDescriptor, clientId: Int, clientCl: ClassLoader): ExecuteOperationResult = {

    // collapse layers like map/filter
    // assign intermediate operations and ID. Refer to intermediate outputs by these IDs

    logDebug("Client submitted operation to execute.")

    val state = clientState.get(clientId)
    if (state == null) {
      throw new IllegalArgumentException("Invalid client ID")
    }

    def assignId(): Int = {
      state.nextStageId.getAndIncrement()
    }

    val stages = new mutable.ListBuffer[Stage]()

    def traverse(t: TransformDescriptor): ExecutorInstruction = {

      t match {
        case MapTransform(input, mapFn) => MapInstruction(traverse(input), mapFn)
        case FlatMapTransform(input, flatMapFn) => FlatMapInstruction(traverse(input), flatMapFn)
        case FilterTransform(input, filterFn) => FilterInstruction(traverse(input), filterFn)
        case RepartitionTransform(input, nPartitions) => {
          // Split up stages
          val in = traverse(input)
          val id = assignId()
          val instr = RepartitionInstruction(in, nPartitions, id)
          stages += Stage(id, instr)

          // Next stage gets its input
          StageInputInstruction(id)
        }
        case ReduceTransform(input, reduceFn) => {
          val in = traverse(input)
          val id = assignId()
          // TODO: we'll pick the reduce executor target up front. Maybe it'd be worth picking this at
          // TODO scheduling time instead...
          val exec = getReduceTargetExecutor()
          val instr = ReduceAndSendInstruction(in, reduceFn, exec, id)
          stages += Stage(id, instr)

          // Beginning of next stage finishes the reduce
          FinishReduceInstruction(StageInputInstruction(id), reduceFn)
        }
        case FirstNTransform(input, n, sortFn) => {
          val in = traverse(input)
          val id = assignId()
          val exec = getReduceTargetExecutor()
          val instr = FirstNInstruction(in, n, sortFn, exec, id)
          stages += Stage(id, instr)

          FinishFirstNInstruction(StageInputInstruction(id), n, sortFn)
        }
        case ClientTextFileSource(fileName) => ClientTextFileInputInstruction(fileName)
        case ClientMultipleTextFilesSource(fileNames) => ClientMultipleTextFileInputInstruction(fileNames)
        case UrlTextFileSource(url) => UrlTextFileInputInstruction(url)
        case UrlMultipleTextFilesSource(urls) => UrlMultipleTextFileInputInstruction(urls)
        case FromSeqSource(data) => FromSeqInputInstruction(data)
        case FromKeysTransform(input) => FromKeysInstruction(traverseGrouped(input))
        case ReduceGroupsTransform(input, reduceFn) => ReduceGroupsInstruction(traverseGrouped(input), reduceFn)
        case MapGroupsTransform(input, mapFn) => MapGroupsInstruction(traverseGrouped(input), mapFn)
        case FlatMapGroupsTransform(input, flatMapFn) => FlatMapGroupsInstruction(traverseGrouped(input), flatMapFn)
      }
    }

    def traverseGrouped(t: TransformDescriptorGrouped): ExecutorInstruction = {
      t match {
        case GroupByTransform(input, keyFn) => {
          // Split groups using consistent hashing.
          // Pick a set of executors, give them an id from Int.MIN_VALUE to Int.MAX_VALUE
          val in = traverse(input)
          val id = assignId()

          val execs = getGroupByExecutors()
          val instr = GroupByInstruction(in, keyFn, execs, id)
          stages += Stage(id, instr)

          FinishGroupBy(StageInputInstruction(id), keyFn)
        }
        case MapValuesTransform(input, mapFn) => MapValuesInstruction(traverseGrouped(input), mapFn)
      }
    }

    val lastInstr = op match {
      case CollectOp(input, limit) =>
        CollectInstruction(traverse(input), limit)

      case CountOp(input) =>
        CountInstruction(traverse(input))

      case CollectGroupedOp(input, limit) =>
        CollectGroupedInstruction(traverseGrouped(input), limit)

      case CountGroupedOp(input) =>
        CountGroupedInstruction(traverseGrouped(input))
    }
    val finalStage = Stage(assignId(), lastInstr)
    stages += finalStage

    logDebug(s"Operation has ${stages.size} stages")
    logDebug(s"Stages:\n${stages.map(_.instructions).mkString("\n")}")

    // Stages is fully populated, can now execute in order!

    val stageDependencies = stages.map(s =>
      (s.stageId,
        getDependenciesFor(s).map(sid =>
          stages.find(_.stageId == sid).get
        ))
    ).toMap

    while (stages.nonEmpty) {
      // Get a ready stage.
      val readyStages = stages.filter(s => {
        val deps = stageDependencies(s.stageId)
        // All dependencies completed
        deps.forall(_.result.nonEmpty)
      })

      stages.filterInPlace(s => {
        !(readyStages.map(_.stageId) contains s.stageId)
      })

      val stagesFuture = Future.sequence(readyStages.map(stage => execStage(stage, stageDependencies(stage.stageId), clientId, clientCl)))

      Await.result(stagesFuture, STAGE_TIMEOUT)
    }

    if (finalStage.result.isEmpty) {
      logErr("After all stages completed, final stage's result is not ready.")
      throw new IllegalStateException("Final stage result not ready")
    }

    ExecuteOperationResult(serialize(finalStage.result.get))
  }

  def execStage(stage: Stage, dependencies: Seq[Stage], clientId: Int, clientCl: ClassLoader): Future[OperationResult] = {
    // Get executor(s)
    // FIXME
    val executorsToRun = getExecutors(stage, dependencies)
    stage.startTime = Some(System.nanoTime())

    executorsToRun transformWith {
      case Failure(throwable) => {
        logErr(s"Could not find executors to run stage ${stage.stageId}", throwable)
        throw throwable
      }
      case Success(execs) => {
        // send stage
        logDebug(s"Starting stage ${stage.stageId}")
        runInstrOn(stage.instructions, execs, clientId)
          .map(taskResults => {
            // Combine results together
            val results = taskResults.map(t => deserialize[OperationResult](t.output, clientCl))
//            logDebug(s"Stage results: $results")
            results.reduce(_ combineWith _)
          }) andThen {
          case Success(result) => {
            stage.endTime = Some(System.nanoTime())
            logDebug(s"Stage ${stage.stageId} has completed. Took ${(stage.endTime.get - stage.startTime.get) / 1e9} seconds.")
            stage.result = Some(result)
          }
          case Failure(throwable) =>
            logErr(s"Stage ${stage.stageId} failed.", throwable)
        }
        // return future of result
      }
    }
  }

  def runInstrOn(instr: ExecutorInstruction, execs: Seq[Int], clientId: Int): Future[Seq[TaskResult]] = {
    if (isNaturallyPartitioned(instr)) {
      Future.sequence(
        execs.zip(partitionInstruction(instr, execs.size)).map {
          case (eid, instr) => executors(eid).stub.runTask(TaskParam(clientId, serialize(instr)))
        }
      )
    } else {
      Future.sequence(
        execs.map(eid => {
          executors(eid).stub.runTask(
            TaskParam(clientId, serialize(instr))
          )
        }))
    }
  }

  def isNaturallyPartitioned(instr: ExecutorInstruction): Boolean = {
    instr match {
      case ClientMultipleTextFileInputInstruction(_) => true
      case UrlMultipleTextFileInputInstruction(_) => true
      case _ => instr.inputs.exists(isNaturallyPartitioned)
    }
  }

  def numNaturalPartitions(instr: ExecutorInstruction): Int = {
    instr match {
      case ClientMultipleTextFileInputInstruction(fileNames) => fileNames.size
      case UrlMultipleTextFileInputInstruction(urls) => urls.size
      case ClientTextFileInputInstruction(_) => 1
      case UrlTextFileInputInstruction(_) => 1
      case StageInputInstruction(_) => 1
      case FromSeqInputInstruction(_) => 1
      // TODO: this sum feels strange... how would this work with something like "union"?
      case _ => instr.inputs.map(numNaturalPartitions).sum
    }
  }

  // Breaks up a Seq into `n` sub-sequences with about as many elements in each sub-sequence
  def breakSeqIntoNParts[T](s: Seq[T], n: Int): Seq[Seq[T]] = {
    if(n <= 1) {
      Seq(s)
    } else {
      val parts = new ListBuffer[ListBuffer[T]]()
      (1 to n).foreach(_ => parts.addOne(new ListBuffer[T]()))
      s.zipWithIndex.foreach {
        case (elem, index) => parts(index % n) += elem
      }
      parts.map(_.toSeq).toSeq
    }

  }

  def partitionInstruction(instr: ExecutorInstruction, n: Int): Seq[ExecutorInstruction] = {
    val numNat = numNaturalPartitions(instr)
    assert(n <= numNat)

    def partition(instr: ExecutorInstruction): Seq[ExecutorInstruction] = {
      instr match {
        case ClientMultipleTextFileInputInstruction(fileNames) => {
          // TODO: Assuming there can only be one multiple file instruction (assert fails if union instruction added)
          assert(fileNames.size == numNat)
          val res = breakSeqIntoNParts(fileNames, n).map(urlSubset =>
            ClientMultipleTextFileInputInstruction(urlSubset)
          )
          assert(res.size == n)
          res
        }
        case UrlMultipleTextFileInputInstruction(urls) => {
          assert(urls.size == numNat)
          val res = breakSeqIntoNParts(urls, n).map(urlSubset =>
            UrlMultipleTextFileInputInstruction(urlSubset)
          )
          assert(res.size == n)
          res
        }
        case MapInstruction(input, m) => partition(input).map(i => MapInstruction(i, m))
        case FlatMapInstruction(input, f) => partition(input).map(i => FlatMapInstruction(i, f))
        case FilterInstruction(input, f) => partition(input).map(i => FilterInstruction(i, f))
        case RepartitionInstruction(input, n, o) => partition(input).map(i => RepartitionInstruction(i, n, o))
        case ReduceAndSendInstruction(input, r, e, o) => partition(input).map(i => ReduceAndSendInstruction(i, r, e, o))
        case FinishReduceInstruction(input, r) => partition(input).map(i => FinishReduceInstruction(i, r))
        case instr: FirstNInstruction => partition(instr.input).map(i => instr.copy(input = i))
        case instr: FinishFirstNInstruction => partition(instr.input).map(i => instr.copy(input = i))
        case CollectInstruction(input, l) => partition(input).map(i => CollectInstruction(i, l))
        case CountInstruction(input) => partition(input).map(i => CountInstruction(i))
        case instr: GroupByInstruction => partition(instr.input).map(i => instr.copy(input = i))
        case FinishGroupBy(input, k) => partition(input).map(i => FinishGroupBy(i, k))
        case MapValuesInstruction(input, m) => partition(input).map(i => MapValuesInstruction(i, m))
        case FromKeysInstruction(input) => partition(input).map(i => FromKeysInstruction(i))
        case ReduceGroupsInstruction(input, r) => partition(input).map(i => ReduceGroupsInstruction(i, r))
        case instr: MapGroupsInstruction => partition(instr.input).map(i => instr.copy(input=i))
        case instr: FlatMapGroupsInstruction => partition(instr.input).map(i => instr.copy(input=i))

        case _ => throw new IllegalArgumentException(s"Invalid instruction encountered ${instr.getClass.getName}")
      }
    }

    val res = partition(instr)
    assert(res.size == n)
    res
  }

  def getExecutors(stage: Stage, dependencies: Seq[Stage]): Future[Seq[Int]] = {

    // TODO: Right now this doesn't check for busy executors. Return type is a Future in case
    // TODO: we want to wait for idle executors before proceeding

    if (dependencies.isEmpty) {
      // FIXME when multiple files/naturally partitioned data
      // Pick any idle executor

      val numExecutors =
        if (isNaturallyPartitioned(stage.instructions))
          numNaturalPartitions(stage.instructions)
        else
          1

      Future.successful(
        executors.values
          .filter(e =>
            e.status match {
              case _: Idle => true
              case _ => false
            }
          )
          .take(numExecutors)
          .map(_.executorId)
          .toSeq
      )
    } else {
      var resultExecutors = Seq[Int]()

      // Check stage dependencies for partitions
      val partitionExecutors = dependencies.flatMap(d => {
        d.result match {
          case Some(RepartitionOpResult(executors)) => executors
          case Some(ReduceAndSendResult(executor)) => Seq(executor)
          case Some(GroupByOpResult(executors)) => executors
          case Some(FirstNResult(executor)) => Seq(executor)
          case _ => Seq()
        }
      }).distinct
      resultExecutors ++= partitionExecutors

      if(resultExecutors.isEmpty) {
        resultExecutors ++=
          executors.values
          .filter(e =>
          e.status match {
            case _: Idle => true
            case _ => false
          }
        ).take(1).map(_.executorId)
      }

      // Perhaps other checks later...
      Future.successful(resultExecutors)

    }
  }

  def getDependenciesFor(stage: Stage): Seq[Int] = {
    val deps = Seq.newBuilder[Int]

    def traverse(i: ExecutorInstruction): Unit = {
      i match {
        case StageInputInstruction(stageId) => deps += stageId
        case _ => i.inputs.foreach(traverse)
      }
    }

    traverse(stage.instructions)
    deps.result()
  }

  def getReduceTargetExecutor(): Int = {
    executors.values
      .filter(e =>
        e.status match {
          case _: Idle => true
          case _ => false
        }
      )
      .head
      .executorId
  }

  def getExecutorsForPartition(request: ExecutorPartitionParams): ExecutorPartitionResult = {
    // TODO make this smarter

    // Give them all the executors!
    ExecutorPartitionResult(executors.values.filter(_.lastSysInfo.nonEmpty).map(_.executorId).toSeq)
  }

  def getGroupByExecutors(): Seq[ExecutorRingPosition] = {
    // Just assigns all executors to the ring
    // TODO: Make this smarter, use "virtual" nodes to weight certain executors
    val execs = executors.values
      .filter(e =>
        e.status match {
          case _: Idle => true
          case _ => false
        }
      )
    if (execs.size == 1) {
      Seq(ExecutorRingPosition(
        execs.head.executorId,
        0
      ))
    } else {
      // Evenly space the n executors around the ring
      val ringSize = abs(Int.MinValue.toLong) + abs(Int.MaxValue.toLong) + 1 // 2^32
      val spaceBetweenExecs = (ringSize / execs.size).toInt // <= 2^31
      var i = 0
      execs.map(_.executorId).map(eid => {
        val pos = ExecutorRingPosition(
          eid,
          Int.MinValue + (spaceBetweenExecs * i)
        )
        i += 1
        pos
      }).toSeq
    }
  }

}
