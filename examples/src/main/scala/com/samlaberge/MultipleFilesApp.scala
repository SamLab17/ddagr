package com.samlaberge

object MultipleFilesApp extends DdagrApp {

  val nUrls = 2

  def getUrl(n: Int): String = {
    val nChars = 12
    s"https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-${s"%0${nChars}d" format n}.csv.gzip"
  }

  val urls = 0 until nUrls map getUrl
  println(s"Using the following ${nUrls} file(s): ")
  println(urls.mkString("\n"))

  val ddagr = new Ddagr("localhost")

  val ds = ddagr.urlTextFiles(urls)

  println(ds.filter(_.nonEmpty).count())
}
