package com.samlaberge

object RedditRPlaceUrls {

  val prefix = "https://www.samlaberge.com/ddagr-data/2022_place_canvas_history-"
  val suffix = ".csv.gzip"
  val formatNum = (n: Int) => "%012d" format n
  val N = 25

  val urls: Seq[String] = (0 until N).map(i => prefix + formatNum(i) + suffix)
}
