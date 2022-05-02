package com.samlaberge

import com.samlaberge.ExampleUtils.{calculateUrlFilesSize, downloadTextFile, mean, stdDev}

import scala.collection.parallel.CollectionConverters._


object LocalWordApp extends App {

  println(s"Number of files: ${BookUrls.urls.length}")
  println(s"Total size: ${calculateUrlFilesSize(BookUrls.urls) / 1024.0 / 1024.0} MB")
  val results = (1 to 5).map(_ => {
    val start = System.nanoTime()
    wordFrequenciesLocal()
    val nSeconds = (System.nanoTime() - start) / 1e9
    println(nSeconds)
    nSeconds
  })
  println(s"Average : ${mean(results)}")
  println(s"stddev : ${stdDev(results)}")

  def wordFrequenciesLocal(): Unit = {
    val data = BookUrls.urls
      .par
      .flatMap(downloadTextFile)

    val start = System.nanoTime()
      data.filter(_.nonEmpty)
      .flatMap(line => line.split("\\s+").map(_.trim).map(_.toLowerCase))
      .filter(w => w.nonEmpty)
      .groupBy(identity)
      .mapValues(_.size)
      .seq
      .toSeq
      .sortBy(-_._2)
      .take(10)
    val nSeconds = (System.nanoTime() - start) / 1e9
    println(nSeconds)

  }

}
