package com.samlaberge

// TransformDescriptors create a new dataset as a result. May have an input (no input if data source)
trait TransformDescriptor {}

// TransformDescriptorGrouped yield GroupedDatasets as a result
trait TransformDescriptorGrouped {}

// Operations trigger computation, typically the result of one or more transformations. There are the top-level
// operations which contain transformations
trait OperationDescriptor {}

// Data Sources
case class FromSeqSource(data: Seq[Any]) extends TransformDescriptor
case class ClientTextFileSource(fileName: String) extends TransformDescriptor
case class ClientMultipleTextFilesSource(fileNames: Seq[String]) extends TransformDescriptor
case class UrlTextFileSource(fileUrl: String) extends TransformDescriptor
case class UrlMultipleTextFilesSource(fileUrls: Seq[String]) extends TransformDescriptor

// Transformations
case class MapTransform(input: TransformDescriptor, mapFn: Any => Any) extends TransformDescriptor
case class FlatMapTransform(input: TransformDescriptor, flatMapFn: Any => IterableOnce[Any]) extends TransformDescriptor
case class FilterTransform(input: TransformDescriptor, filterFn: Any => Boolean) extends TransformDescriptor
case class ReduceTransform(input: TransformDescriptor, reduceFn: (Any, Any) => Any) extends TransformDescriptor

case class FirstNTransform(input: TransformDescriptor, n: Int, sortFn: (Any, Any) => Boolean) extends TransformDescriptor
case class RepartitionTransform(input: TransformDescriptor, nPartitions: Int) extends TransformDescriptor

case class GroupByTransform(input: TransformDescriptor, keyFn: Any => Any) extends TransformDescriptorGrouped
case class MapValuesTransform(input: TransformDescriptorGrouped, mapFn: Any => Any) extends TransformDescriptorGrouped

case class ReduceGroupsTransform(input: TransformDescriptorGrouped, reduceFn: (Any, Any) => Any) extends TransformDescriptor
case class FromKeysTransform(input: TransformDescriptorGrouped) extends TransformDescriptor
case class MapGroupsTransform(input: TransformDescriptorGrouped, mapFn: (Any, Iterator[Any]) => Any) extends TransformDescriptor
case class FlatMapGroupsTransform(input: TransformDescriptorGrouped, flatMapFn: (Any, Iterator[Any]) => IterableOnce[Any]) extends TransformDescriptor

// Operations
case class CountOp(input: TransformDescriptor) extends OperationDescriptor
case class CountGroupedOp(input: TransformDescriptorGrouped) extends OperationDescriptor

case class CollectOp(input: TransformDescriptor, limit: Option[Int]) extends OperationDescriptor
case class CollectGroupedOp(input: TransformDescriptorGrouped, limit: Option[Int]) extends OperationDescriptor
