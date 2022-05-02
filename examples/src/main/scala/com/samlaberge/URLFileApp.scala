package com.samlaberge

object URLFileApp extends DdagrApp {
  val ddagr = new Ddagr("localhost")

  val c = ddagr
    .urlTextFile("https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-000000000000.csv.gzip")
    .count()

  println(s"Number of lines in csv file: $c")
}
