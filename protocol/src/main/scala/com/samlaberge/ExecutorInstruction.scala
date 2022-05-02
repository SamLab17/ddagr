package com.samlaberge

// An instruction to run on an executor.
// Forms an expression tree with other instructions.
sealed trait ExecutorInstruction {
  // The inputs for this instruction, if any
  def inputs: Seq[ExecutorInstruction]
}

case class StageInputInstruction(stageId: Int) extends ExecutorInstruction {
  def inputs = Seq()
}

case class FromSeqInputInstruction(data: Seq[Any]) extends ExecutorInstruction {
  def inputs = Seq()
}

case class ClientTextFileInputInstruction(fileName: String) extends ExecutorInstruction {
  def inputs = Seq()
}

case class ClientMultipleTextFileInputInstruction(fileNames: Seq[String]) extends ExecutorInstruction {
  def inputs = Seq()
}

case class UrlTextFileInputInstruction(fileUrl: String) extends ExecutorInstruction {
  def inputs = Seq()
}

case class UrlMultipleTextFileInputInstruction(urls: Seq[String]) extends ExecutorInstruction {
  def inputs = Seq()
}

case class MapInstruction(input: ExecutorInstruction, mapFn: Any => Any) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FlatMapInstruction(input: ExecutorInstruction, flatMapFn: Any => IterableOnce[Any]) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FilterInstruction(input: ExecutorInstruction, filterFn: Any => Boolean) extends ExecutorInstruction {
  def inputs = Seq(input)
}

// Reduce will be done in two steps:
// 1. All executors with data will perform a reduce locally and send their results to single executor
// <Stage change>
// 2. Executor with all data will perform the final reduce and continue
// This has the advantage that it can be scaled to multiple, incremental reduce steps

case class ReduceAndSendInstruction(input: ExecutorInstruction, reduceFn: (Any, Any) => Any, execId: Int, outputId: Int) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FinishReduceInstruction(input: ExecutorInstruction, reduceFn: (Any, Any) => Any) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class RepartitionInstruction(input: ExecutorInstruction, nPartitions: Int, outputId: Int) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class ExecutorRingPosition (
  executorId: Int,
  ringPosition: Int
)

case class GroupByInstruction(input: ExecutorInstruction, keyFn: Any => Any, executors: Seq[ExecutorRingPosition], outputId: Int) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FinishGroupBy(input: ExecutorInstruction, keyFn: Any => Any) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FirstNInstruction(input: ExecutorInstruction, n: Int, sortFn: (Any, Any) => Boolean, executor: Int, outputId: Int) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FinishFirstNInstruction(input: ExecutorInstruction, n: Int, sortFn: (Any, Any) => Boolean) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class MapValuesInstruction(input: ExecutorInstruction, mapFn: Any => Any) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FromKeysInstruction(input: ExecutorInstruction) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class MapGroupsInstruction(input: ExecutorInstruction, mapFn: (Any, Iterator[Any]) => Any) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class FlatMapGroupsInstruction(input: ExecutorInstruction, flatMapFn: (Any, Iterator[Any]) => IterableOnce[Any]) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class ReduceGroupsInstruction(input: ExecutorInstruction, reduceFn: (Any, Any) => Any) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class CollectInstruction(input: ExecutorInstruction, limit: Option[Int]) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class CountInstruction(input: ExecutorInstruction) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class CollectGroupedInstruction(input: ExecutorInstruction, limit: Option[Int]) extends ExecutorInstruction {
  def inputs = Seq(input)
}

case class CountGroupedInstruction(input: ExecutorInstruction) extends ExecutorInstruction {
  def inputs = Seq(input)
}
