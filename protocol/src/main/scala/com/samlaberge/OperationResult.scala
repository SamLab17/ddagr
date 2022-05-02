package com.samlaberge

// The result of a stage executed on an executor.
sealed trait OperationResult {
  // When multiple executors are executing the same stage, we need to combine their
  // results together somehow.
  def combineWith(other: OperationResult): OperationResult
}

case class CollectOpResult(result: Seq[_], limit: Option[Int]) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case CollectOpResult(otherRes, otherLimit) => {
        if(limit != otherLimit) {
          throw new IllegalArgumentException(s"Attempted to combine two Collects with different limits: $limit, $otherLimit")
        }
        limit match {
          case Some(lim) => {
            if(result.size >= lim)  {
              this
            } else {
              CollectOpResult(result ++ otherRes.take(lim - result.size), limit)
            }
          }
          case None => CollectOpResult(result ++ otherRes, None)
        }
      }
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

case class FirstNResult(executor: Int) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case FirstNResult(otherExec) => {
        if(executor != otherExec) throw new IllegalArgumentException(s"Top N with different executors: $executor, $otherExec")
        this
      }
      case _ => throw new IllegalArgumentException(s"Attempted to combine $this and $other")
    }
  }
}

case class CollectGroupedOpResult(result: Map[Any, Any], limit: Option[Int]) extends OperationResult {
  override def combineWith(other: OperationResult): OperationResult = {
    other match {
      case CollectGroupedOpResult(other, otherLimit) => {
        // Combine the two maps together, check for conflicts
        if(limit != otherLimit) {
          throw new IllegalArgumentException(s"Attempted to combine two Collects with different limits: $limit, $otherLimit")
        }
        limit match {
          case Some(limit) => {
            if(result.size >= limit) {
              this
            } else {
              val combined = result ++ other.take(limit - result.size)
              if(combined.size != limit) {
                throw new IllegalArgumentException(s"Two grouped collects had conflicting groups: ${result.keySet}, ${other.keySet}")
              }
              CollectGroupedOpResult(combined, None)
            }
          }
          case None => {
            val combined = result ++ other
            if (combined.size != other.size + result.size) {
              throw new IllegalArgumentException(s"Two grouped collects had conflicting groups: ${result.keySet}, ${other.keySet}")
            }
            CollectGroupedOpResult(combined, None)
          }
        }
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