package com.samlaberge

sealed trait OperationResult {
  def combineWith(other: OperationResult): OperationResult
}

case class CollectOpResult(result: Seq[_]) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case CollectOpResult(otherRes) => CollectOpResult(result ++ otherRes)
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}

case class CountOpResult(result: Int) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case CountOpResult(otherRes) => CountOpResult(result + otherRes)
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}

// List of executor Ids that partitions were sent to
case class RepartitionOpResult(executors: Seq[Int]) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case RepartitionOpResult(otherExecutors) => RepartitionOpResult((executors ++ otherExecutors).distinct)
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}

case class GroupByOpResult(executors: Seq[Int]) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case GroupByOpResult(otherExec) => GroupByOpResult((executors ++ otherExec).distinct)
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}


case class ReduceAndSendResult(executor: Int) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case ReduceAndSendResult(otherExec) => {
        if(executor != otherExec) throw new IllegalArgumentException(s"Reduced to different executors: $executor, $otherExec")
        this
      }
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}

case class CollectGroupedOpResult(result: Map[Any, Any]) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case CollectGroupedOpResult(other) => {
        // Combine the two maps together, check for conflicts
        val combined = result ++ other
        if (combined.size != other.size + result.size) {
          throw new IllegalArgumentException(s"Two grouped collects had conflicting groups: ${result.keySet}, ${other.keySet}")
        }
        CollectGroupedOpResult(combined)
      }
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}

case class CountGroupedOpResult(result: Int) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case CountGroupedOpResult(other) => CountGroupedOpResult(other + result)
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}