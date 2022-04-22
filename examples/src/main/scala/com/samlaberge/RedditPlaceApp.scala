package com.samlaberge

object RedditPlaceApp extends DdagrApp {

//  val ddagr = new Ddagr(DdagrOptions("backgammon.cs.utexas.edu"))
    val ddagr = new Ddagr(DdagrOptions("localhost"))

  val nUrls = 3

  def getUrl(n: Int): String = {
    val nChars = 12
    s"https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-${s"%0${nChars}d" format n}.csv.gzip"
  }

  val urls = RedditRPlaceUrls.urls.slice(0, nUrls)

//  val urls = 0 until nUrls map getUrl
  println(s"Using the following ${nUrls} file(s): ")
  println(urls.mkString("\n"))

  val ds = ddagr.multipleUrlTextFiles(urls)

  case class Row(
    time: String,
    user: String,
    color: String,
    posX: Int,
    posY: Int
  )

  val start = System.nanoTime()

  val transforms = ds
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
    .groupBy(r => {
      (r.posX, r.posY)
    })
    .mapValues(_ => 1)
    .reduceGroups(_ + _)
    .filter(_._2 > 20000)

  val result = transforms.collect()
    .sortBy(_._2 * -1)

  val end = System.nanoTime()

  println(s"Most popular 50x50 regions in r/place: ${result.mkString("\n")}")

  println(s"Num elements: ${result.size}")
  println(s"Max: ${result.maxBy(_._2)}")
  println(s"Min: ${result.minBy(_._2)}")
  println(s"Result hashcode: ${result.hashCode()}")

  println(s"Total elapsed time: ${(end - start) / 1e9} seconds")

  ddagr.exit()
}
