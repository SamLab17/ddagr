package com.samlaberge

trait GroupedDataset[K, T] {
  def ddagr: Ddagr

  def keys(): Dataset[K] = {
    Dataset.FromKeysDataset(ddagr, this)
  }

  def mapValues[U](mapFn: T => U): GroupedDataset[K, U] = {
    GroupedDataset.MappedValuesDataset(ddagr, this, mapFn)
  }

  def mapGroups[U](mapFn: (K, Iterator[T]) => U): Dataset[U] = {
    Dataset.MappedGroupsDataset(ddagr, this, mapFn)
  }

  def flatMapGroups[U](flatMapFn: (K, Iterator[T]) => IterableOnce[U]): Dataset[U] = {
    Dataset.FlatMappedGroupsDataset(ddagr, this, flatMapFn)
  }

  def reduceGroups(reduceFn: (T, T) => T): Dataset[(K, T)] = {
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
