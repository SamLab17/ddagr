package com.samlaberge

import org.apache.commons.io.IOUtils

import java.io.ByteArrayInputStream
import java.net.URL
import java.util.zip.GZIPInputStream
import scala.collection.parallel.CollectionConverters._
import scala.io.Source
import Numeric.Implicits._

object ExampleUtils {

  def downloadTextFile(url: String): Iterator[String] = {
    Source.fromBytes(downloadFile(url)).getLines()
  }

  def downloadCompressedFile(url: String): Iterator[String] = {
    Source.fromInputStream(new GZIPInputStream(new ByteArrayInputStream(downloadFile(url)))).getLines()
  }

  def twoDecimals(n: Double): String = {
    "%.2f" format n
  }

  private def downloadFile(url: String, silent: Boolean = true): Array[Byte] = {
    val start = System.nanoTime()
    val res = IOUtils.toByteArray(new URL(url))
    val nSeconds = (System.nanoTime() - start) / 1e9
    if (!silent)
      println(s"Downloaded $url in ${twoDecimals(nSeconds)} seconds (${twoDecimals(res.length / 1024.0)}KB @ ${twoDecimals(res.length / 1024.0 / 1024.0 / nSeconds)} MB/s)")
    res
  }

  def calculateUrlFilesSize(urls: Seq[String]): Long = {
    urls.par.map(u => downloadFile(u, silent = true)).map(_.length.toLong).sum
  }

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

}
