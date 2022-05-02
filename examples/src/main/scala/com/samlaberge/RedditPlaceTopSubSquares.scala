package com.samlaberge

object RedditPlaceTopSubSquares extends DdagrApp {

  val ddagr = new Ddagr("frosted-mini-wheats.cs.utexas.edu")

  val ds = ddagr.urlTextFiles(RedditRPlaceUrls.urls)

  val transforms = ds
    .filter(_.nonEmpty)
    .filter(_.split(",").length == 5)
    .map(line => {
      val split = line.split(",")
      val x = split(3).replaceAll("\"", "").toInt / 50 * 50
      val y = split(4).replaceAll("\"", "").toInt / 50 * 50
      (x, y)
    })
    .groupBy(identity)
    .mapGroups((pos, entries) => (pos, entries.size))

  val result = transforms.collect()

  println(s"Most popular 50x50 regions in r/place: ${result.mkString("\n")}")

  println(s"Num elements: ${result.size}")
  println(s"Max: ${result.maxBy(_._2)}")
  println(s"Min: ${result.minBy(_._2)}")

  ddagr.exit()
}
