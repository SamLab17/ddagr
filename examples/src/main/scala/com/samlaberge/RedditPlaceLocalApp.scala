package com.samlaberge

import java.io.{ByteArrayInputStream, FileInputStream}
import java.util.zip.GZIPInputStream
import org.apache.commons.io.IOUtils

import java.net.URL
import java.util.concurrent.atomic.AtomicInteger
import scala.io.Source
import scala.collection.parallel.CollectionConverters._

// This is the local-only version of the reddit r/place data job

object RedditPlaceLocalApp extends App {
//  println(RedditRPlaceUrls.urls.mkString("\n"))

  def getDecompressedFile(n: Int): String =
    s"/Users/sam/Desktop/ddagr-data/2022_place_canvas_history-${"%012d" format n}.csv"
  def getCompressedFile(n: Int): String =
    getDecompressedFile(n) + ".gzip"


//  val urls = 0 until nUrls map getUrl
  val urls = RedditRPlaceUrls.urls.take(20)
  val compressedFiles = (0 until 20).map(getCompressedFile).toVector
  val decompressedFiles = (0 until 20).map(getDecompressedFile).toVector

  case class Row(
//    time: String,
//    user: String,
//    color: String,
    posX: Int,
    posY: Int
  )

  val numDownloaded = new AtomicInteger(0);

  val start = System.nanoTime()
//
//  val downloaded = urls
////    .par
//    .flatMap(url => {
//      val downloadStart = System.nanoTime()
//      val bytes = IOUtils.toByteArray(new URL(url))
//      val downloadEnd = System.nanoTime()
//      val downloadTime = (downloadEnd - downloadStart) / 1e9
//
//      val res = Source.fromInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes))).getLines()
//      println(s"download complete. ${numDownloaded.incrementAndGet()}/${urls.size}, ${downloadTime} seconds (${bytes.length / 1024.0 / 1024.0 / downloadTime} MB/s)")
//      res
//    })

//  val downloaded = compressedFiles.flatMap(fileName => {
//    Source.fromInputStream(new GZIPInputStream(new FileInputStream(fileName))).getLines()
//  })
  val downloaded = decompressedFiles.flatMap(f => Source.fromFile(f).getLines())


  val doneDownload = System.nanoTime()
  println(s"All downloaded: ${(doneDownload - start) / 1e9} seconds elapsed")
    val result = downloaded
    .filter(_.nonEmpty)
    .filter(_.split(",").length == 5)
    .map(line => {
      val split = line.split(",")
      //      val posYString = split(4)
      Row(
//        time = split(0),
//        user = split(1),
//        color = split(2),
        posX = split(3).replaceAll("\"", "").toInt / 50 * 50,
        posY = split(4).replaceAll("\"", "").toInt / 50 * 50
      )
    })
//      .seq
    .groupBy(r => (r.posX, r.posY))
      .view
    .mapValues(_.size)
    .filter(_._2 > 10000)
      .toSeq
//    .seq
//    .toSeq
    .sortBy(_._2 * -1)

  val end = System.nanoTime()

//  println(s"Most popular 50x50 regions in r/place: ${result.mkString("\n")}")
//
//  println(s"Num elements: ${result.size}")
//  println(s"Max: ${result.maxBy(_._2)}")
//  println(s"Min: ${result.minBy(_._2)}")
//  println(s"Result hashcode: ${result.hashCode()}")
  println(s"Processing time: ${(end - doneDownload)/ 1e9} seconds")
  println(s"Total elapsed time: ${(end - start) / 1e9} seconds")
}
