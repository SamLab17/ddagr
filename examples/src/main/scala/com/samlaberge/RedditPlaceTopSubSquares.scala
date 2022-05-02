package com.samlaberge

object RedditPlaceTopSubSquares extends DdagrApp {

//  val host = sys.env.getOrElse("SCHEDULER", "localhost")
  val host = "frosted-mini-wheats.cs.utexas.edu"
  val ddagr = new Ddagr(host)

  val urls = RedditRPlaceUrls.urls.take(20)

  val ds = ddagr.urlTextFiles(urls)

  case class Row(
    posX: Int,
    posY: Int
  )

  val start = System.nanoTime()

  val transforms = ds
    .filter(_.nonEmpty)
    .filter(_.split(",").length == 5)
    .map(line => {
      val split = line.split(",")
      Row(
        posX = split(3).replaceAll("\"", "").toInt / 50 * 50,
        posY = split(4).replaceAll("\"", "").toInt / 50 * 50
      )
    })
    .groupBy(r => {
      (r.posX, r.posY)
    })
    .mapGroups((pos, entries) => (pos, entries.size))
    .firstNBy(500, _._2, descending = true)

  val result = transforms.collect()
  //  val result = transforms.count()
  //  println(s"Num entries: ${result}")
  val end = System.nanoTime()

  //    println(s"Most popular 50x50 regions in r/place: ${result.mkString("\n")}")
  //
  //    println(s"Num elements: ${result.size}")
  //    println(s"Max: ${result.maxBy(_._2)}")
  //    println(s"Min: ${result.minBy(_._2)}")
  //    println(s"Result hashcode: ${result.hashCode()}")
  //
  //    println(s"Total elapsed time: ${(end - start) / 1e9} seconds")
  println(s"${(end - start) / 1e9} seconds")

  ddagr.exit()

}
