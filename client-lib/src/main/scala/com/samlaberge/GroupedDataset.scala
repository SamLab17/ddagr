package com.samlaberge

trait GroupedDataset[K, T] {
  def ddagr: Ddagr

  def keys(): Dataset[K] = {
    Dataset.FromKeysDataset(ddagr, this)
  }

  def mapValues[U](mapFn: T => U): GroupedDataset[K, U] = {
    ClosureCleaner.clean(mapFn)
    GroupedDataset.MappedValuesDataset(ddagr, this, mapFn)
  }

  def reduceGroups(reduceFn: (T, T) => T): Dataset[(K, T)] = {
    ClosureCleaner.clean(reduceFn)
    Dataset.ReducedGroupsDataset(ddagr, this, reduceFn)
  }

  def count(): Int = {
    ddagr.doCountGrouped(this)
  }

  def collect(limit: Option[Int] = None): Map[K, T] = {
    ddagr.doCollectGrouped(this, limit)
  }

}

object GroupedDataset {
  case class GroupByDataset[K, T](ddagr: Ddagr, src: Dataset[T], keyFn: T => K) extends GroupedDataset[K, T]
  case class MappedValuesDataset[K, T, U](ddagr: Ddagr, src: GroupedDataset[K, T], mapFn: T => U) extends GroupedDataset[K, U]
}
