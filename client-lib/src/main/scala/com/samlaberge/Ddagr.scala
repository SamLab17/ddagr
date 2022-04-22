package com.samlaberge

import com.samlaberge.SystemConfig.{MAX_MESSAGE_SIZE, PORTS}
import com.samlaberge.Util._
import com.samlaberge.protos.scheduler.SchedulerClientGrpc.SchedulerClientBlockingStub
import com.samlaberge.protos.scheduler._
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.io.{File, FileNotFoundException}
import java.net.{InetAddress, URL}


class Ddagr(options: DdagrOptions)(implicit callingObject: DdagrApp) extends Logging {

  // Start up Ddagr client

  // Init with scheduler
  /*_*/
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(options.schedulerIp, options.schedulerPort)
    .maxInboundMessageSize(MAX_MESSAGE_SIZE)
    .usePlaintext()
    .build
  /*_*/
  private val schedulerStub: SchedulerClientBlockingStub = SchedulerClientGrpc.blockingStub(channel)
  private val initResult: DdagrClientInitResult = schedulerStub.clientInit(
    DdagrClientInitParams(
      clientFileServicePort = options.localPort,
      clientIp = InetAddress.getLocalHost.getHostAddress,
    )
  )

  private val classDir = callingObject.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
  private val fileClient = new FileLookupClient(options.schedulerIp, initResult.fileServerPort, classDir)
  private var cleanedUp = false

  sys addShutdownHook {
    if(!cleanedUp)
      this.exit()
  }

  def exit(): Unit = {
    schedulerStub.clientDisconnect(ClientDisconnectParams(initResult.clientId))
    fileClient.stopServer()
    cleanedUp = true
    System.exit(0)
  }

  def from[T](data: Seq[T]): Dataset[T] = {
    Dataset.FromSeq(this, data)
  }

  def localTextFile(filePath: String): Dataset[String] = {
    val file = new File(filePath)
    if (!file.exists()) {
      throw new FileNotFoundException(s"File ${file.getPath} does not exist")
    }
    Dataset.LocalTextFile(this, file.getPath)
  }

  def multipleLocalTextFiles(filePaths: Seq[String]): Dataset[String] = {
    filePaths.foreach(f => {
      if (!new File(f).exists())
        throw new FileNotFoundException(s"File $f does not exist")
    })
    Dataset.MultipleLocalTextFiles(this, filePaths)
  }

  def urlTextFile(fileUrl: String): Dataset[String] = {
    // Check if fileUrl is valid
    new URL(fileUrl).openStream().close()
    Dataset.UrlTextFile(this, fileUrl)
  }

  def multipleUrlTextFiles(fileUrls: Seq[String]): Dataset[String] = {
    fileUrls.foreach(url => new URL(url).openStream().close())
    Dataset.MultipleUrlTextFiles(this, fileUrls)
  }

  private def convertDataset(d: Dataset[_]): TransformDescriptor = {
    d match {
      case Dataset.FromSeq(_, data) => FromSeqSource(data)
      case Dataset.LocalTextFile(_, file) => ClientTextFileSource(file)
      case Dataset.MultipleLocalTextFiles(_, files) => ClientMultipleTextFilesSource(files)
      case Dataset.UrlTextFile(_, url) => UrlTextFileSource(url)
      case Dataset.MultipleUrlTextFiles(_, urls) => UrlMultipleTextFilesSource(urls)
      case Dataset.MappedDataset(_, src, mapFn) => MapTransform(convertDataset(src), mapFn.asInstanceOf[Any => Any])
      case Dataset.FlatMappedDataset(_, src, flatMapFn) => FlatMapTransform(convertDataset(src), flatMapFn.asInstanceOf[Any => Iterable[Any]])
      case Dataset.FilteredDataset(_, src, filterFn) => FilterTransform(convertDataset(src), filterFn.asInstanceOf[Any => Boolean])
      case Dataset.RepartitionedDataset(_, src, n) => RepartitionTransform(convertDataset(src), n)
      case Dataset.ReducedDataset(_, src, reduceFn) => ReduceTransform(convertDataset(src), reduceFn.asInstanceOf[(Any, Any) => Any])
      case Dataset.TopNDataset(_, src, n, sortFn) => FirstNTransform(convertDataset(src), n, sortFn.asInstanceOf[(Any, Any) => Boolean])
      case Dataset.FromKeysDataset(_, src) => FromKeysTransform(convertGroupedDataset(src))
      case Dataset.ReducedGroupsDataset(_, src, reduceFn) => ReduceGroupsTransform(convertGroupedDataset(src), reduceFn.asInstanceOf[(Any, Any) => Any])
      case Dataset.MappedGroupsDataset(_, src, mapFn) => MapGroupsTransform(convertGroupedDataset(src), mapFn.asInstanceOf[(Any, Iterator[Any]) => Any])
      case Dataset.FlatMappedGroupsDataset(_, src, flatMapFn) => FlatMapGroupsTransform(convertGroupedDataset(src), flatMapFn.asInstanceOf[(Any, Iterator[Any]) => IterableOnce[Any]])
    }
  }

  private def convertGroupedDataset[K, T](d: GroupedDataset[K, T]): TransformDescriptorGrouped = {
    d match {
      case GroupedDataset.GroupByDataset(_, src, keyFn) => GroupByTransform(convertDataset(src), keyFn.asInstanceOf[Any => Any])
      case GroupedDataset.MappedValuesDataset(_, src, mapFn) => MapValuesTransform(convertGroupedDataset(src), mapFn.asInstanceOf[Any => Any])
    }
  }

  // Have call back methods for Datasets to call when an action (.count, .collect, .save) is invoked

  def doCollect[T](d: Dataset[T], limit: Option[Int]): Seq[T] = {
    val op = CollectOp(convertDataset(d), limit)
    val res = schedulerStub.executeOperation(ExecuteOperationParams(
      initResult.clientId,
      serialize(op)
    ))
    deserialize[OperationResult](res.resultData) match {
      case CollectOpResult(result, _) => result.asInstanceOf[Seq[T]]
      case _ => throw new IllegalStateException("Wrong result type returned by scheduler.")
    }
  }

  def doCount[T](d: Dataset[T]): Int = {
    val op = CountOp(convertDataset(d))
    val res = schedulerStub.executeOperation(ExecuteOperationParams(
      initResult.clientId,
      serialize(op)
    ))
    deserialize[OperationResult](res.resultData) match {
      case CountOpResult(result) => result
      case _ => throw new IllegalStateException("Wrong result type returned by scheduler.")
    }
  }


  def doCollectGrouped[K, T](d: GroupedDataset[K, T], limit: Option[Int]): Map[K, T] = {
    val op = CollectGroupedOp(convertGroupedDataset(d), limit)
    val res = schedulerStub.executeOperation(ExecuteOperationParams(
      initResult.clientId,
      serialize(op)
    ))
    deserialize[OperationResult](res.resultData) match {
      case CollectGroupedOpResult(result, _) => result.asInstanceOf[Map[K, T]]
      case _ => throw new IllegalStateException("Wrong result type returned by scheduler.")
    }
  }

  def doCountGrouped[K, T](d: GroupedDataset[K, T]): Int = {
    val op = CountGroupedOp(convertGroupedDataset(d))
    val res = schedulerStub.executeOperation(ExecuteOperationParams(
      initResult.clientId,
      serialize(op)
    ))
    deserialize[OperationResult](res.resultData) match {
      case CountGroupedOpResult(result) => result
    }
  }

}

case class DdagrOptions(
  schedulerIp: String,
  schedulerPort: Int = PORTS.SCHEDULER_CLIENT_SERVER,
  localPort: Int = PORTS.CLIENT_PORT,
)
