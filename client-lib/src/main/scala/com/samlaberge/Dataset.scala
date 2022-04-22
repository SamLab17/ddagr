package com.samlaberge

import com.samlaberge.Dataset.{ReducedDataset, TopNDataset}

trait Dataset[T] {

  def ddagr: Ddagr

  def map[U](mapFn: T => U): Dataset[U] = {
    Dataset.MappedDataset(ddagr, this, mapFn)
  }

  def flatMap[U](flatMapFn: T => IterableOnce[U]): Dataset[U] = {
    Dataset.FlatMappedDataset(ddagr, this, flatMapFn)
  }

  def filter(filterFn: T => Boolean): Dataset[T] = {
    Dataset.FilteredDataset(ddagr, this, filterFn)
  }

  def repartition(n: Int): Dataset[T] = {
    Dataset.RepartitionedDataset(ddagr, this, n)
  }

  def groupBy[K](keyFn: T => K): GroupedDataset[K, T] = {
    GroupedDataset.GroupByDataset(ddagr, this, keyFn)
  }

  def reduce(reduceFn: (T, T) => T): Dataset[T] = {
    ReducedDataset(ddagr, this, reduceFn)
  }

  def firstN(n: Int, lt: (T, T) => Boolean): Dataset[T] = {
    TopNDataset(ddagr, this, n, lt)
  }

  def firstNBy[U](n: Int, keyFn: T => U, descending: Boolean = false)(implicit ord: Ordering[U]): Dataset[T] = {
    val ltFn = if(descending) {
      (lhs: T, rhs: T) => ord.gt(keyFn(lhs), keyFn(rhs))
    } else {
      (lhs: T, rhs: T) => ord.lt(keyFn(lhs), keyFn(rhs))
    }
    TopNDataset(ddagr, this, n, ltFn)
  }

  def collect(limit: Option[Int] = None): Seq[T] = {
    ddagr.doCollect(this, limit)
  }

  def count(): Int = {
    ddagr.doCount(this)
  }


  // flatMap
  // reduce
  // groupBy -> GroupedDataset[K, T]
  // keys -> Dataset[K]
  // reduceGroups -> Dataset[(K, T)]
  // mapValues -> Dataset[K, U]
}

object Dataset {
  // Source Datasets
  case class FromSeq[T](ddagr: Ddagr, data: Seq[T]) extends Dataset[T]
  case class LocalTextFile(ddagr: Ddagr, file: String) extends Dataset[String]
  case class MultipleLocalTextFiles(ddagr: Ddagr, files: Seq[String]) extends Dataset[String]
  case class UrlTextFile(ddagr: Ddagr, url: String) extends Dataset[String]
  case class MultipleUrlTextFiles(ddagr: Ddagr, urls: Seq[String]) extends Dataset[String]

  // Result Datasets
  case class MappedDataset[T, U](ddagr: Ddagr, src: Dataset[U], mapFn: U => T) extends Dataset[T]
  case class FlatMappedDataset[T, U](ddagr: Ddagr, src: Dataset[U], flatMapFn: U => IterableOnce[T]) extends Dataset[T]
  case class FilteredDataset[T](ddagr: Ddagr, src: Dataset[T], filterFn: T => Boolean) extends Dataset[T]
  case class RepartitionedDataset[T](ddagr: Ddagr, src: Dataset[T], n: Int) extends Dataset[T]
  case class ReducedDataset[T](ddagr: Ddagr, src: Dataset[T], reduceFn: (T, T) => T) extends Dataset[T]
  case class TopNDataset[T](ddagr: Ddagr, src: Dataset[T], n: Int, lt: (T, T) => Boolean) extends Dataset[T]


  case class FromKeysDataset[K, T](ddagr: Ddagr, src: GroupedDataset[K, T]) extends Dataset[K]
  case class ReducedGroupsDataset[K, U](ddagr: Ddagr, src: GroupedDataset[K, U], reduceFn: (U, U) => U) extends Dataset[(K, U)]
  case class MappedGroupsDataset[K, U, T](ddagr: Ddagr, src: GroupedDataset[K, T], mapFn: (K, Iterator[T]) => U) extends Dataset[U]
  case class FlatMappedGroupsDataset[K, U, T](ddagr: Ddagr, src: GroupedDataset[K, T], flatMapFn: (K, Iterator[T]) => IterableOnce[U]) extends Dataset[U]
}
