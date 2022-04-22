package com.samlaberge

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import org.apache.commons.io.IOUtils

import java.net.URL
import scala.io.Source
import scala.collection.parallel.CollectionConverters._

// This is the local-only version of the reddit r/place data job

object RedditPlaceLocalApp extends App {
  val nUrls = 1

  def getUrl(n: Int): String = {
    val nChars = 12
    s"https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-${s"%0${nChars}d" format n}.csv.gzip"
  }

  println(RedditRPlaceUrls.urls.mkString("\n"))

//  val urls = 0 until nUrls map getUrl
  val urls = RedditRPlaceUrls.urls.take(nUrls)
  println(s"Using the following ${nUrls} file(s): ")
  println(urls.mkString("\n"))

  println(urls.flatMap(url =>
    Source.fromInputStream(new GZIPInputStream(new ByteArrayInputStream(IOUtils.toByteArray(new URL(url))))).getLines()
  ).take(10))

  case class Row(
    time: String,
    user: String,
    color: String,
    posX: Int,
    posY: Int
  )

  val start = System.nanoTime()

  val result = urls
    .par
    .flatMap(url => {
      println(s"downloading $url")
      Source.fromInputStream(new GZIPInputStream(new ByteArrayInputStream(IOUtils.toByteArray(new URL(url))))).getLines()
    })
    .filter(_.nonEmpty)
    .filter(_.split(",").length == 5)
    .map(line => {
      val split = line.split(",")
      //      val posYString = split(4)
      Row(
        time = split(0),
        user = split(1),
        color = split(2),
        posX = split(3).replaceAll("\"", "").toInt / 50 * 50,
        posY = split(4).replaceAll("\"", "").toInt / 50 * 50
      )
    })
    .groupBy(r => (r.posX, r.posY))
    .mapValues(_.size)
    .filter(_._2 > 10000)
    .seq
    .toSeq
    .sortBy(_._2 * -1)

  val end = System.nanoTime()

  println(s"Most popular 50x50 regions in r/place: ${result.mkString("\n")}")

  println(s"Num elements: ${result.size}")
  println(s"Max: ${result.maxBy(_._2)}")
  println(s"Min: ${result.minBy(_._2)}")
  println(s"Result hashcode: ${result.hashCode()}")

  println(s"Total elapsed time: ${(end - start) / 1e9} seconds")
}
